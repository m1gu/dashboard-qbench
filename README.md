# QBench Dashboard

Aplicación de escritorio construida con PySide6 para visualizar métricas recientes de la plataforma QBench.

## Características
- Autenticación contra la API de QBench usando PyJWT y `requests`.
- Consulta de muestras, tests y clientes con manejo de paginación y reintentos.
- Métricas clave en la cabecera: Samples, Tests, Customers, Reports y TAT promedio.
- Gráfico de líneas interactivo para samples y tests con tooltips.
- Selector de rango de fechas con validaciones flexibles (sin límite fijo de días).

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

### Opciones de DATA_PROVIDER
- `qbench`: usa las credenciales OAuth de QBench y la API remota oficial.
- `local`: apunta al servicio local (`http://localhost:8000`) pensado para desarrollo.
- `online`: reutiliza los mismos endpoints que `local` pero con el tunel remoto `https://615c98lc-8000.use.devtunnels.ms`.

En todos los casos se puede sobrescribir la URL base en `.env`: usa `LOCAL_API_BASE_URL` cuando `DATA_PROVIDER=local` y `ONLINE_API_BASE_URL` cuando `DATA_PROVIDER=online` (con retrocompatibilidad hacia `LOCAL_API_BASE_URL`). El valor se normaliza automaticamente para eliminar un `/` final si estuviera presente.

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

## Distribucion como ejecutable (.exe)
1. Instala PyInstaller: `pip install pyinstaller`.
2. Ejecuta `python build_exe.py` desde la raíz del proyecto (genera `dist/MCRLabsDashboard/`).
3. Distribuye el contenido de `dist/MCRLabsDashboard/` y utiliza `MCRLabsDashboard.exe`.

### Notas del build
- El paquete congelado fuerza `DATA_PROVIDER=online` y usa `https://615c98lc-8000.use.devtunnels.ms` por defecto.
- Antes de abrir la ventana, el ejecutable verifica la conectividad con un timeout corto; si falla, muestra una alerta y termina.
- Los clientes HTTP reducen reintentos y timeouts en modo empaquetado para evitar bloqueos al perder internet.
- Se recomienda ejecutar el `.exe` en un entorno con conexión estable; la carpeta `dist` incluye todas las dependencias necesarias.
