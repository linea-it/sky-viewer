# Deploy Production environment

Clone the repository to temp folder, copy docker-compose.production.yml, env template and ngnix conf to application folder, remove temp folder, edit env file change the settings and image tag, edit docker compose and mapping port if necessary.

Steps Considering 

```bash
Host: srvnode01
User: app.deployer
application port: 8087 
application folder: /apps/skyviewer.
```
Copy files and create directories

```bash
mkdir -p skyviewer skyviewer/logs skyviewer/data/redis skyviewer/data/tmp skyviewer/certificates \
&& chmod -R g+w skyviewer/logs skyviewer/data \
&& git clone https://github.com/linea-it/sky-viewer.git skyviewer_temp \
&& cp skyviewer_temp/compose/production/docker-compose.production.yml skyviewer/docker-compose.yml \ 
&& cp skyviewer_temp/compose/production/env_template skyviewer/.env \ 
&& cp skyviewer_temp/compose/production/nginx-proxy.conf skyviewer/nginx-proxy.conf
&& rm -rf skyviewer_temp \
&& cd skyviewer \
&& docker compose pull frontend \
&& docker compose up -d
```

Generate SAML2 Certificates

```bash
cd certificates \
&& openssl genrsa -out mykey.key 2048 \
&& openssl req -new -key mykey.key -out mycert.csr \
&& openssl x509 -req -days 365 -in mycert.csr -signkey mykey.key -out mycert.crt \
&& cp mykey.key mykey.pem \
&& cp mycert.crt mycert.pem \
&& cd ..
&& chmod -R go+r certificates \
```

Edit .env for secrets, users and passwords.

Up all services in background

```bash
docker compose up -d
```

Generate a secret for Django 
```bash
docker compose exec -it backend python -c "import secrets; print(secrets.token_urlsafe())"
```
Copy the secret and edit .env, replace template DJANGO_SECRET_KEY

Create Django superuser
```bash
docker compose exec backend python manage.py createsuperuser
```

Restart all Services
```bash
docker compose down && docker compose up -d
```



