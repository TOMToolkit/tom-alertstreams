"""
Django settings for tom_alertstreams_base project.

Generated by 'django-admin startproject' using Django 4.1.1.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.1/ref/settings/
"""
import logging.config
import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-mk4(4w3(pleb6&du#qo*7##l(3&lwt$lhmzyhyw!2)1(p-7o!e'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'tom_alertstreams'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'tom_alertstreams_base.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'tom_alertstreams_base.wsgi.application'


# Database
# https://docs.djangoproject.com/en/4.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# Password validation
# https://docs.djangoproject.com/en/4.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.1/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/4.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'handlers': ['console'],
        # 'level': 'WARNING',
        'level': 'INFO',
        # 'level': 'DEBUG',
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
            'propagate': False,
        },
    },
}
logging.config.dictConfig(LOGGING)

#
# tom_alertstreams configuration
#
ALERT_STREAMS = [
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.hopskotch.HopskotchAlertStream',
        'OPTIONS': {
            'URL': 'kafka://kafka.scimma.org/',
            'USERNAME': os.getenv('SCIMMA_AUTH_USERNAME', None),
            'PASSWORD': os.getenv('SCIMMA_AUTH_PASSWORD', None),
            'TOPIC_HANDLERS': {
                'sys.heartbeat': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
                'sys.heartbeatnew': 'tom_alertstreams.alertstreams.hopskotch.heartbeat_handler',
                'tomtoolkit.test': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
                'hermes.test': 'tom_alertstreams.alertstreams.hopskotch.alert_logger',
            },
        },
    },
    {
        'ACTIVE': True,
        'NAME': 'tom_alertstreams.alertstreams.gcn.GCNClassicAlertStream',
        # The keys of the OPTIONS dictionary become (lower-case) properties of the AlertStream instance.
        'OPTIONS': {
            # see https://github.com/nasa-gcn/gcn-kafka-python#to-use for configuration details.
            'GCN_CLASSIC_CLIENT_ID': os.getenv('GCN_CLASSIC_CLIENT_ID', None),
            'GCN_CLASSIC_CLIENT_SECRET': os.getenv('GCN_CLASSIC_CLIENT_SECRET', None),
            'DOMAIN': 'gcn.nasa.gov',  # optional, defaults to 'gcn.nasa.gov'
            'CONFIG': {  # optional
                # 'group.id': 'tom_alertstreams-my-custom-group-id',
                # 'auto.offset.reset': 'earliest',
                # 'enable.auto.commit': False
            },
            'TOPIC_HANDLERS': {
                'gcn.classic.text.LVC_INITIAL': 'tom_alertstreams.alertstreams.gcn.alert_logger',
                'gcn.classic.text.LVC_PRELIMINARY': 'tom_alertstreams.alertstreams.gcn.alert_logger',
                'gcn.classic.text.LVC_RETRACTION': 'tom_alertstreams.alertstreams.gcn.alert_logger',
                'gcn.classic.text.LVC_TEST': 'tom_alertstreams.alertstreams.gcn.alert_logger',
            },
        },
    }
]


try:
    logging.info('Looking for local_settings.py')
    from local_settings import *  # noqa
except ImportError:
    pass
