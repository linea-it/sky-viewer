# Sky Viewer

LIneA Sky Viewer

## Setup Production Environment
https://github.com/linea-it/sky-viewer/blob/main/compose/production/README.md


## Build Manual do frontend e backend para utilizar no -dev
```bash
docker build -f compose/production/frontend/Dockerfile -t linea/skyviewer:frontend_$(git describe --always) .

docker build -f compose/production/django/Dockerfile -t linea/skyviewer:backend_$(git describe --always) .
```

## Build e push pelo GitHub Actions
Em **Actions → Docker Build and Push → Run workflow**, escolha a imagem (`all`, `backend` ou `frontend`). O workflow faz o build e o push para o Docker Hub com as mesmas tags do build manual (`linea/skyviewer:<imagem>_<git describe>`).

Requer os secrets `DOCKERHUB_USERNAME` e `DOCKERHUB_TOKEN` configurados no repositório.
