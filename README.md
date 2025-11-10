# Proyecto de Prueba de Concepto en Python para Google Cloud Run

Este proyecto es una prueba de concepto que despliega una aplicación simple en Google Cloud Run utilizando integración continua desde GitHub a Google Cloud Build. La aplicación responde a un endpoint y registra "ok function" en los logs cada vez que se llama.

## Estructura del Proyecto

```
cloudrun-python-poc
├── src
│   ├── main.py          # Punto de entrada de la aplicación
│   └── requirements.txt  # Dependencias del proyecto
├── .github
│   └── workflows
│       └── cloudbuild.yml # Configuración de CI/CD con GitHub Actions
├── Dockerfile            # Instrucciones para construir la imagen Docker
├── README.md             # Documentación del proyecto
└── .gcloudignore         # Archivos a ignorar en el despliegue
```

## Requisitos

Asegúrate de tener instalado lo siguiente:

- Python 3.x
- Docker
- Google Cloud SDK
- GitHub CLI (opcional, para facilitar la integración con GitHub)

## Instalación

1. Clona el repositorio:
   ```
   git clone <URL_DEL_REPOSITORIO>
   cd cloudrun-python-poc
   ```

2. Instala las dependencias:
   ```
   pip install -r src/requirements.txt
   ```

## Despliegue

Para desplegar la aplicación en Google Cloud Run, sigue estos pasos:

1. Configura Google Cloud SDK y autentícate:
   ```
   gcloud auth login
   gcloud config set project <ID_DEL_PROYECTO>
   ```

2. Construye y despliega la aplicación:
   ```
   gcloud builds submit --tag gcr.io/<ID_DEL_PROYECTO>/cloudrun-python-poc
   gcloud run deploy --image gcr.io/<ID_DEL_PROYECTO>/cloudrun-python-poc --platform managed
   ```

## Uso

Una vez desplegada, puedes llamar al endpoint de la aplicación. Cada vez que se llame, se registrará "ok function" en los logs de Google Cloud.

## Contribuciones

Las contribuciones son bienvenidas. Si deseas contribuir, por favor abre un issue o envía un pull request.

## Licencia

Este proyecto está bajo la Licencia MIT.