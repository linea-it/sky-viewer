# ruff: noqa: E501
import logging
import os

import saml2
import saml2.saml

# import sentry_sdk
# from sentry_sdk.integrations.celery import CeleryIntegration
# from sentry_sdk.integrations.django import DjangoIntegration
# from sentry_sdk.integrations.logging import LoggingIntegration
# from sentry_sdk.integrations.redis import RedisIntegration
from .base import *  # noqa: F403
from .base import DATABASES
from .base import INSTALLED_APPS
from .base import SPECTACULAR_SETTINGS
from .base import env

# GENERAL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#secret-key
SECRET_KEY = env("DJANGO_SECRET_KEY")

# Complete URL of the production server with protocol and port
BASE_HOST = env("BASE_HOST", default="http://skyviewer.linea.org.br")
# Url without protocol (Necessário para SAML2)
DOMAIN = env("DOMAIN", default="skyviewer.linea.org.br")

# https://docs.djangoproject.com/en/dev/ref/settings/#allowed-hosts
ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS", default=["skyviewer.linea.org.br"])

# DATABASES
# ------------------------------------------------------------------------------
DATABASES["default"]["CONN_MAX_AGE"] = env.int("CONN_MAX_AGE", default=60)

# CACHES
# ------------------------------------------------------------------------------
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": env("REDIS_URL"),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            # Mimicing memcache behavior.
            # https://github.com/jazzband/django-redis#memcached-exceptions-behavior
            "IGNORE_EXCEPTIONS": True,
        },
    },
}

# # SECURITY
# # ------------------------------------------------------------------------------
# # https://docs.djangoproject.com/en/dev/ref/settings/#secure-proxy-ssl-header
# SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
# # https://docs.djangoproject.com/en/dev/ref/settings/#secure-ssl-redirect
# SECURE_SSL_REDIRECT = env.bool("DJANGO_SECURE_SSL_REDIRECT", default=True)
# # https://docs.djangoproject.com/en/dev/ref/settings/#session-cookie-secure
# SESSION_COOKIE_SECURE = True
# # https://docs.djangoproject.com/en/dev/ref/settings/#session-cookie-name
# SESSION_COOKIE_NAME = "__Secure-sessionid"
# # https://docs.djangoproject.com/en/dev/ref/settings/#csrf-cookie-secure
# CSRF_COOKIE_SECURE = True
# # https://docs.djangoproject.com/en/dev/ref/settings/#csrf-cookie-name
# CSRF_COOKIE_NAME = "__Secure-csrftoken"
# # https://docs.djangoproject.com/en/dev/topics/security/#ssl-https
# # https://docs.djangoproject.com/en/dev/ref/settings/#secure-hsts-seconds
# # TODO: set this to 60 seconds first and then to 518400 once you prove the former works
# SECURE_HSTS_SECONDS = 60
# # https://docs.djangoproject.com/en/dev/ref/settings/#secure-hsts-include-subdomains
# SECURE_HSTS_INCLUDE_SUBDOMAINS = env.bool(
#     "DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS",
#     default=True,
# )
# # https://docs.djangoproject.com/en/dev/ref/settings/#secure-hsts-preload
# SECURE_HSTS_PRELOAD = env.bool("DJANGO_SECURE_HSTS_PRELOAD", default=True)
# # https://docs.djangoproject.com/en/dev/ref/middleware/#x-content-type-options-nosniff
# SECURE_CONTENT_TYPE_NOSNIFF = env.bool(
#     "DJANGO_SECURE_CONTENT_TYPE_NOSNIFF",
#     default=True,
# )

SESSION_COOKIE_DOMAIN  = ".linea.org.br"
CSRF_COOKIE_DOMAIN = ".linea.org.br"

# STATIC & MEDIA
# ------------------------
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

# EMAIL
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#default-from-email
DEFAULT_FROM_EMAIL = env(
    "DJANGO_DEFAULT_FROM_EMAIL",
    default="Sky Viewer <noreply@skyviewer.linea.org.br>",
)
# https://docs.djangoproject.com/en/dev/ref/settings/#server-email
SERVER_EMAIL = env("DJANGO_SERVER_EMAIL", default=DEFAULT_FROM_EMAIL)
# https://docs.djangoproject.com/en/dev/ref/settings/#email-subject-prefix
EMAIL_SUBJECT_PREFIX = env(
    "DJANGO_EMAIL_SUBJECT_PREFIX",
    default="[Sky Viewer] ",
)

# ADMIN
# ------------------------------------------------------------------------------
# Django Admin URL regex.
ADMIN_URL = env("DJANGO_ADMIN_URL", default="admin/")

# Anymail
# ------------------------------------------------------------------------------
# https://anymail.readthedocs.io/en/stable/installation/#installing-anymail
INSTALLED_APPS += ["anymail"]
# https://docs.djangoproject.com/en/dev/ref/settings/#email-backend
# https://anymail.readthedocs.io/en/stable/installation/#anymail-settings-reference
# https://anymail.readthedocs.io/en/stable/esps
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
ANYMAIL = {}

# django-compressor
# ------------------------------------------------------------------------------
# https://django-compressor.readthedocs.io/en/latest/settings/#django.conf.settings.COMPRESS_ENABLED
COMPRESS_ENABLED = env.bool("COMPRESS_ENABLED", default=True)
# https://django-compressor.readthedocs.io/en/latest/settings/#django.conf.settings.COMPRESS_STORAGE
COMPRESS_STORAGE = "compressor.storage.GzipCompressorFileStorage"
# https://django-compressor.readthedocs.io/en/latest/settings/#django.conf.settings.COMPRESS_URL
COMPRESS_URL = STATIC_URL  # noqa: F405
# https://django-compressor.readthedocs.io/en/latest/settings/#django.conf.settings.COMPRESS_OFFLINE
COMPRESS_OFFLINE = True  # Offline compression is required when using Whitenoise
# https://django-compressor.readthedocs.io/en/latest/settings/#django.conf.settings.COMPRESS_FILTERS
COMPRESS_FILTERS = {
    "css": [
        "compressor.filters.css_default.CssAbsoluteFilter",
        "compressor.filters.cssmin.rCSSMinFilter",
    ],
    "js": ["compressor.filters.jsmin.JSMinFilter"],
}

# LOGGING
# ------------------------------------------------------------------------------
# https://docs.djangoproject.com/en/dev/ref/settings/#logging
# See https://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.

LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "verbose": {
            "format": "%(asctime)s [%(levelname)s] %(message)s",
        },
    },
    "handlers": {
        "console": {
            "level": LOG_LEVEL,
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "default": {
            "level": LOG_LEVEL,
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "django.log"),
            "formatter": "verbose",
        },
        "djangosaml2": {
            "level": LOG_LEVEL,
            "class": "logging.handlers.RotatingFileHandler",
            "maxBytes": 1024 * 1024 * 5,  # 5 MB
            "backupCount": 5,
            "filename": os.path.join(LOG_DIR, "djangosaml2.log"),
            "formatter": "verbose",
        },        
    },
    "root": {"level": "INFO", "handlers": ["console"]},
    "loggers": {
        "django": {
            "level": LOG_LEVEL, 
            "handlers": ["default", "console"], 
            "propagate": True
        },
        "django.db.backends": {
            "level": "ERROR",
            "handlers": ["console"],
            "propagate": False,
        },
        # Errors logged by the SDK itself
        "sentry_sdk": {"level": "ERROR", "handlers": ["console"], "propagate": False},
        "django.security.DisallowedHost": {
            "level": "ERROR",
            "handlers": ["console"],
            "propagate": False,
        },
        "djangosaml2": {
            "level": LOG_LEVEL, 
            "handlers": ["djangosaml2"], 
            "propagate": True
        },
    },
}

# # Sentry
# # ------------------------------------------------------------------------------
# SENTRY_DSN = env("SENTRY_DSN")
# SENTRY_LOG_LEVEL = env.int("DJANGO_SENTRY_LOG_LEVEL", logging.INFO)

# sentry_logging = LoggingIntegration(
#     level=SENTRY_LOG_LEVEL,  # Capture info and above as breadcrumbs
#     event_level=logging.ERROR,  # Send errors as events
# )
# integrations = [
#     sentry_logging,
#     DjangoIntegration(),
#     CeleryIntegration(),
#     RedisIntegration(),
# ]
# sentry_sdk.init(
#     dsn=SENTRY_DSN,
#     integrations=integrations,
#     environment=env("SENTRY_ENVIRONMENT", default="production"),
#     traces_sample_rate=env.float("SENTRY_TRACES_SAMPLE_RATE", default=0.0),
# )

# django-rest-framework
# -------------------------------------------------------------------------------
# Tools that generate code samples can use SERVERS to point to the correct domain
SPECTACULAR_SETTINGS["SERVERS"] = [
    {"url": BASE_HOST, "description": "Production server"},
]

# ------------------------------------------------------------------------------
# Your stuff...
# ------------------------------------------------------------------------------

# Qualquer view que requer um usuário autenticado deve redirecionar o navegador para esta url
LOGIN_URL = "/api/login"
# Urls for login with SAML2/CILogon
# URL_CILOGON example: https://skyviewer.linea.org.br/saml2/login/?idp=https://satosa.linea.org.br/linea/proxy/aHR0cHM6Ly9jaWxvZ29uLm9yZw==
LINEA_LOGIN_URL = env("LINEA_LOGIN_URL")
RUBIN_LOGIN_URL = env("RUBIN_LOGIN_URL")

# # COmanage Autorization
# # ------------------------------------------------------------------------------
# COMANAGE_SERVER_URL = os.environ.get(
#     "COMANAGE_SERVER_URL", "https://register.linea.org.br"
# )
# COMANAGE_USER = os.environ.get("COMANAGE_USER", "co_2.linea.apps")
# COMANAGE_PASSWORD = os.environ.get("COMANAGE_PASSWORD")
# COMANAGE_COID = os.environ.get("COMANAGE_COID")

# Django SAML2
# ------------------------------------------------------------------------------
# Declarado no inicio do arquivo.
# DOMAIN = env("DOMAIN", default="targetviewer.linea.org.br")

CONFIG_DIR = Path(BASE_DIR).joinpath("config")

# FQDN Exemplo:https://targetviewer.linea.org.br
FQDN = BASE_HOST

CERT_DIR = CONFIG_DIR.joinpath("certificates")
ATTR_DIR = CONFIG_DIR.joinpath("attribute-maps")

# Including SAML2 Backend Authentication
# AUTHENTICATION_BACKENDS += ("djangosaml2.backends.Saml2Backend", )
# Custom Saml2 Backend for LIneA
AUTHENTICATION_BACKENDS += ("common.saml2.LineaSaml2Backend",)
# Including SAML2 Middleware
MIDDLEWARE += ("djangosaml2.middleware.SamlSessionMiddleware",)
# SAML2 Custom error handler
# https://djangosaml2.readthedocs.io/contents/developer.html#custom-error-handler
SAML_ACS_FAILURE_RESPONSE_FUNCTION = 'common.views.saml2_template_failure'
# configurações relativas ao session cookie
SAML_SESSION_COOKIE_NAME = "saml_session"
SESSION_COOKIE_SECURE = True

# Encerra a sessão quando o usuário fecha o navegador
SESSION_EXPIRE_AT_BROWSER_CLOSE = False

# Tipo de binding utilizado
SAML_DEFAULT_BINDING = saml2.BINDING_HTTP_POST
SAML_IGNORE_LOGOUT_ERRORS = True

# Cria usuário Django a partir da asserção SAML caso o mesmo não exista
SAML_CREATE_UNKNOWN_USER = True

# https://djangosaml2.readthedocs.io/contents/security.html#content-security-policy
SAML_CSP_HANDLER = ""

# URL para redirecionamento após a autenticação
LOGIN_REDIRECT_URL = "/"

SAML_ATTRIBUTE_MAPPING = {
    "eduPersonUniqueId": ("username",),
    "givenName": ("first_name",),
    "sn": ("last_name",),
    "email": ("email",),
    "isMemberOf": ("name",),
}

SAML_CONFIG = {
    # Biblioteca usada para assinatura e criptografia
    "xmlsec_binary": "/usr/bin/xmlsec1",
    "entityid": FQDN + "/saml2/metadata/",
    # Diretório contendo os esquemas de mapeamento de atributo
    "attribute_map_dir": str(ATTR_DIR),
    "description": "SP Sky Viewer",
    "service": {
        "sp": {
            "name": "SP Sky Viewer",
            "ui_info": {
                "display_name": {"text": "SP Sky Viewer", "lang": "en"},
                "description": {"text": "SP Sky Viewer", "lang": "en"},
                "information_url": {"text": FQDN, "lang": "en"},
                "privacy_statement_url": {"text": FQDN, "lang": "en"},
            },
            "name_id_format": [
                "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
                "urn:oasis:names:tc:SAML:2.0:nameid-format:transient",
            ],
            # Indica os endpoints dos serviços fornecidos
            "endpoints": {
                "assertion_consumer_service": [
                    (FQDN + "/saml2/acs/", saml2.BINDING_HTTP_POST),
                ],
                "single_logout_service": [
                    (FQDN + "/saml2/ls/", saml2.BINDING_HTTP_REDIRECT),
                    (FQDN + "/saml2/ls/post", saml2.BINDING_HTTP_POST),
                ],
            },
            # Algoritmos utilizados
            #'signing_algorithm':  saml2.xmldsig.SIG_RSA_SHA256,
            #'digest_algorithm':  saml2.xmldsig.DIGEST_SHA256,
            "force_authn": False,
            "name_id_format_allow_create": False,
            # Indica que as respostas de autenticação para este SP devem ser assinadas
            "want_response_signed": True,
            # Indica se as solicitações de autenticação enviadas por este SP devem ser assinadas
            "authn_requests_signed": True,
            # Indica se este SP deseja que o IdP envie as asserções assinadas
            "want_assertions_signed": False,
            "only_use_keys_in_metadata": True,
            "allow_unsolicited": False,
        },
    },
    # Indica onde os metadados podem ser encontrados
    "metadata": {
        "remote": [
            {
                "url": "https://www.linea.org.br/static/metadata/satosa-prod-frontend-cilogon.xml",
                "cert": None,
            },
            {
                "url": "https://www.linea.org.br/static/metadata/satosa-dev-frontend-cilogon.xml",
                "cert": None,
            },
            {
                "url": "https://www.linea.org.br/static/metadata/satosa-dev-frontend-rubin.xml",
                "cert": None,
            },
            {
                "url": "https://www.linea.org.br/static/metadata/satosa-prod-frontend-rubin.xml",
                "cert": None,
            },            
        ],
    },
    # Configurado como 1 para fornecer informações de debug
    "debug": 1,
    # Signature
    "key_file": str(CERT_DIR.joinpath("mykey.pem")),  # private part
    "cert_file": str(CERT_DIR.joinpath("mycert.pem")),  # public part
    # Encriptation
    "encryption_keypairs": [
        {
            "key_file": str(CERT_DIR.joinpath("mykey.pem")),  # private part
            "cert_file": str(CERT_DIR.joinpath("mycert.pem")),  # public part
        }
    ],
    "contact_person": [
        {
            "given_name": "Service",
            "sur_name": "Desk",
            "company": "LIneA",
            "email_address": "helpdesk@linea.org.br",
            "contact_type": "technical",
        },
    ],
    # Descreve a organização responsável pelo serviço
    "organization": {
        "name": [("LIneA", "pt-br")],
        "display_name": [("LIneA", "pt-br")],
        "url": [("https://www.linea.org.br", "pt-br")],
    },
}
