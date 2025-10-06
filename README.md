# Sky Viewer

LIneA Sky Viewer

## Setup Production Environment
https://github.com/linea-it/sky-viewer/blob/main/compose/production/README.md


## Build Manual do frontend e backend para utilizar no -dev
```bash
docker build -f compose/production/frontend/Dockerfile -t linea/skyviewer:frontend_$(git describe --always) .

docker build -f compose/production/django/Dockerfile -t linea/skyviewer:backend_$(git describe --always) .
```

