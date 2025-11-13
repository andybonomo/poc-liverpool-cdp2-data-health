import os
import json
from flask import Flask, request, jsonify
from google.cloud import bigquery
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
client = bigquery.Client()
PROJECT_ID = os.environ.get("GCP_PROJECT", client.project) 
MAX_WORKERS = 10  # Número máximo de tablas a procesar en paralelo

def process_single_table(source_dataset_id, table_id):
    """Lógica para perfilar una única tabla."""
    full_table_id = f"{PROJECT_ID}.{source_dataset_id}.{table_id}"
    report_data = []

    try:
        # 1. Obtener el esquema
        table_ref = client.get_table(full_table_id)
        fields = [(field.name, field.field_type) for field in table_ref.schema]
        
        # 2. Construir la consulta dinámica para contar nulos
        select_parts = [f"COUNTIF({field_name} IS NULL) AS null_{field_name}_count" 
                        for field_name, _ in fields]
        
        query = f"""
        SELECT 
            COUNT(1) AS total_rows, 
            {', '.join(select_parts)}
        FROM `{full_table_id}`
        LIMIT 1
        """
        
        # 3. Ejecutar la consulta (I/O Bound)
        query_job = client.query(query)
        results = list(query_job.result())
        
        if not results:
            return report_data, f"Tabla vacía: {table_id}"

        row_data = results[0]
        total_rows = row_data['total_rows']
        
        if total_rows == 0:
            return report_data, f"Tabla vacía: {table_id}"

        # 4. Calcular porcentajes y armar el reporte
        for field_name, field_type in fields:
            null_count_key = f"null_{field_name}_count"
            null_count = row_data[null_count_key]
            null_percentage = (null_count / total_rows) * 100
            
            report_data.append({
                'table_id': table_id,
                'column_name': field_name,
                'column_type': field_type,
                'total_rows': total_rows,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2)
            })

        return report_data, None

    except Exception as e:
        error_msg = f"Error en tabla {table_id}: {str(e)}"
        print(error_msg)
        return [], error_msg


def get_null_percentage_report(source_dataset_id, destination_dataset_id):
    """Coordina el procesamiento paralelo de todas las tablas."""
    
    # 1. Obtener lista de tablas
    tables = client.list_tables(source_dataset_id)
    table_ids = [table.table_id for table in tables]
    
    if not table_ids:
        return f"No se encontraron tablas en el dataset: {source_dataset_id}", 404

    all_reports = []
    
    # 2. Usar ThreadPoolExecutor para procesamiento concurrente
    # Como la tarea es I/O-bound (esperar a BQ), los threads son eficientes.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Mapear la función de procesamiento a cada tabla
        future_to_table = {executor.submit(process_single_table, source_dataset_id, tid): tid 
                           for tid in table_ids}
        
        # Esperar y procesar resultados a medida que llegan
        for future in as_completed(future_to_table):
            table_id = future_to_table[future]
            try:
                report_data, error = future.result()
                if error:
                    # Opcional: registrar el error en el reporte final
                    pass 
                all_reports.extend(report_data)
            except Exception as e:
                print(f"La tabla {table_id} generó una excepción: {e}")

    # 3. Guardar el reporte en BigQuery (igual que la versión anterior)
    if not all_reports:
        return "Proceso finalizado. No se generó reporte (tablas vacías o errores).", 200

    df_report = pd.DataFrame(all_reports)
    destination_table_id = "column_null_report" 
    table_ref = f"{PROJECT_ID}.{destination_dataset_id}.{destination_table_id}"
    
    # Escribir en BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_report, table_ref, job_config=job_config) 
    job.result()  
    
    return f"Reporte de {len(table_ids)} tablas guardado en: {table_ref}", 200


@app.route("/", methods=["GET", "POST"])
def index():
    # Lógica de manejo de parámetros HTTP (sin cambios)
    if request.method == "POST":
        try:
            data = request.get_json() or request.form
        except:
            data = request.form
    else:
        data = request.args

    source_ds = data.get("dataset-a-revisar")
    dest_ds = data.get("dataset-destino")

    if not source_ds or not dest_ds:
        return (
            jsonify({"error": "Faltan parámetros. Se requiere 'dataset-a-revisar' y 'dataset-destino'."}),
            400,
        )

    print(f"Iniciando revisión concurrente del Dataset: {source_ds}...")
    message, status_code = get_null_percentage_report(source_ds, dest_ds)
    print(f"Resultado: {message} (Código: {status_code})")
    
    return jsonify({"message": message}), status_code

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
