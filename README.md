# QBench Dashboard

Aplicación de escritorio construida con PySide6 para visualizar métricas recientes de la plataforma QBench.

## Características
- Autenticación contra la API de QBench usando PyJWT y `requests`.
- Consulta de muestras, tests y clientes con manejo de paginación y reintentos.
- Métricas clave en la cabecera: Samples, Tests, Customers, Reports y TAT promedio.
- Gráfico de líneas interactivo para samples y tests con tooltips.
- Selector de rango de fechas con validaciones (máximo 30 días).

## Requisitos
- Python 3.10+
- Dependencias listadas en `requirements.txt`.

## Instalación
```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate
pip install -r requirements.txt
```

## Uso
```bash
python app.py
```

Configura las variables de entorno requeridas en `.env` (ver `qbench_dashboard/config.py`).

## Estructura principal
```
app.py                     # Punto de entrada
qbench_dashboard/
├── config.py              # Carga de ajustes desde .env
├── services/
│   ├── qbench_client.py   # Cliente HTTP y normalización de datos
│   └── summary.py         # Agregación de métricas para la UI
└── ui/
    └── main_window.py     # Interfaz PySide6 y lógica de presentación
```

## Desarrollo
- Ejecuta `python -m py_compile ...` para validaciones rápidas.
- Usa `git status` para inspeccionar cambios antes de hacer commit.

