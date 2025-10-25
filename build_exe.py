from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parent
    spec_path = project_root / "MCRLabsDashboard.spec"
    if not spec_path.exists():
        raise SystemExit("No se encontró MCRLabsDashboard.spec en el directorio raíz del proyecto.")

    build_dir = project_root / "build"
    dist_dir = project_root / "dist"
    for path in (build_dir, dist_dir):
        if path.exists():
            shutil.rmtree(path)

    cmd = [sys.executable, "-m", "PyInstaller", "--clean", str(spec_path)]
    print("Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print("Build completado. Revisa:", dist_dir / "MCRLabsDashboard")


if __name__ == "__main__":
    main()
