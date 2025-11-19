import os
import json
from flask import Flask, request, jsonify
from google.cloud import bigquery
import pandas as pd
# Eliminamos ThreadPoolExecutor ya que el procesamiento es ahora de una sola tabla.

app = Flask(__name__)
client = bigquery.Client()
PROJECT_ID = os.environ.get("GCP_PROJECT", client.project)
# MAX_WORKERS ya no es necesario.

def process_single_table(source_dataset_id, table_id):
    """Lógica para perfilar una única tabla. (Mantenemos el nombre de la función original)"""
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
            # Usar un pequeño valor epsilon para evitar ZeroDivisionError en caso de bug,
            # aunque total_rows=0 ya se maneja arriba.
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


def get_null_percentage_report(source_dataset_id, source_table_id, destination_dataset_id, destination_table_id):
    """
    Procesa una única tabla de origen y guarda el reporte en una tabla de destino específica.
    """
    
    # Llama directamente a la función de procesamiento de una sola tabla.
    report_data, error = process_single_table(source_dataset_id, source_table_id)

    if error:
        # Retorna el error de la tabla específica
        return f"Fallo al procesar {source_table_id}: {error}", 500

    if not report_data:
        # Si la tabla está vacía o no se generó reporte.
        return f"Proceso finalizado. La tabla {source_table_id} está vacía.", 200

    # 3. Guardar el reporte en BigQuery
    df_report = pd.DataFrame(report_data)
    
    # El destino ahora es completamente dinámico (dataset + nombre de tabla de destino)
    table_ref = f"{PROJECT_ID}.{destination_dataset_id}.{destination_table_id}"
    
    # Escribir en BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_report, table_ref, job_config=job_config) 
    job.result()  
    
    return f"Reporte de la tabla {source_table_id} guardado en: {table_ref}", 200


@app.route("/", methods=["GET", "POST"])
def index():
    # Lógica de manejo de parámetros HTTP
    if request.method == "POST":
        try:
            data = request.get_json() or request.form
        except:
            data = request.form
    else:
        data = request.args

    # Nuevos parámetros requeridos
    source_ds = data.get("dataset-a-revisar")
    source_tbl = data.get("tabla-a-revisar")
    dest_ds = data.get("dataset-destino")
    dest_tbl = data.get("tabla-destino") # Nuevo nombre de tabla destino
    
    if not all([source_ds, source_tbl, dest_ds, dest_tbl]):
        return (
            jsonify({
                "error": "Faltan parámetros. Se requiere 'dataset-a-revisar', 'tabla-a-revisar', 'dataset-destino' y 'tabla-destino'."
            }),
            400,
        )

    print(f"Iniciando revisión de la tabla de origen: {source_ds}.{source_tbl}")
    print(f"El reporte se guardará en la tabla destino: {dest_ds}.{dest_tbl}")

    # Pasamos los cuatro parámetros a la función de reporte
    message, status_code = get_null_percentage_report(source_ds, source_tbl, dest_ds, dest_tbl)
    print(f"Resultado: {message} (Código: {status_code})")
    
    return jsonify({"message": message}), status_code

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
