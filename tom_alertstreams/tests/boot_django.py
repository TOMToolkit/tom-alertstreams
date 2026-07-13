# boot_django.py
#
# This file sets up and configures Django. It's used by scripts that need to
# execute as if running in a Django server.

import os
import django
from django.conf import settings

APP_NAME = 'tom_alertstreams'  # the stand-alone app we are testing

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), APP_NAME))


def boot_django():
    settings.configure(
        BASE_DIR=BASE_DIR,
        # SECURITY WARNING: keep the secret key used in production secret!
        SECRET_KEY='v5j-rg7sc+leg-m+vf947vi34+fs1%+$m%*l%sb7^fnwb$-29y',
        DEBUG=True,
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
            }
        },
        INSTALLED_APPS=(
            'django.contrib.admin',
            'django.contrib.auth',
            'django.contrib.contenttypes',
            'django.contrib.sessions',
            'django.contrib.messages',
            'django.contrib.staticfiles',
            'django.contrib.sites',
            'django_extensions',
            # Rendering dependencies for the Recent Alerts page. These are lightweight,
            # self-contained apps (no app-registry cascade), so the view tests can render
            # the page's own template without installing the full TOM stack. tom_common
            # itself is only imported (htmx_table), never an installed app — the page's
            # base.html is satisfied by the stub in tests/templates/tom_common/base.html.
            'crispy_forms',
            'crispy_bootstrap4',
            'django_filters',
            'django_tables2',
            'django_htmx',
            APP_NAME,  # defined above
        ),
        # crispy_forms needs a template pack to render {% crispy filter.form %};
        # bootstrap4 matches the TOM's CRISPY_TEMPLATE_PACK.
        CRISPY_TEMPLATE_PACK='bootstrap4',
        # Mount only the alertstreams URLs (under the 'alertstreams' namespace) so
        # reverse('alertstreams:recent-alerts') and {% url 'alertstreams:topic-choices' %}
        # resolve in view tests. See tests/urls.py.
        ROOT_URLCONF='tom_alertstreams.tests.urls',
        EXTRA_FIELDS={},
        # tom_alertstreams opt-in: the AppConfig's include_url_paths() and nav_items()
        # only register the Recent Alerts URL/navbar when this is True, so the tests
        # that exercise those integration points need it set in the test settings.
        SHOW_RECENT_ALERTS=True,
        TIME_ZONE='UTC',
        USE_TZ=True,
        MIDDLEWARE=[
            'django.middleware.security.SecurityMiddleware',
            'django.contrib.sessions.middleware.SessionMiddleware',
            'django.middleware.common.CommonMiddleware',
            'django.middleware.csrf.CsrfViewMiddleware',
            'django.contrib.auth.middleware.AuthenticationMiddleware',
            'django.contrib.messages.middleware.MessageMiddleware',
            'django.middleware.clickjacking.XFrameOptionsMiddleware',
            # Sets request.htmx, which HTMXTableViewMixin inspects on the view.
            'django_htmx.middleware.HtmxMiddleware',
        ],
        TEMPLATES=[
            {
                'BACKEND': 'django.template.backends.django.DjangoTemplates',
                # The second DIR holds the stub tom_common/base.html so the Recent
                # Alerts page can render without tom_common in INSTALLED_APPS.
                'DIRS': [
                    os.path.join(BASE_DIR, 'templates'),
                    os.path.join(os.path.dirname(__file__), 'templates'),
                ],
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
        ],
        AUTHENTICATION_BACKENDS=(
            'django.contrib.auth.backends.ModelBackend',
        ),
        AUTH_STRATEGY='READ_ONLY',
        STATIC_URL='/static/',
        STATIC_ROOT=os.path.join(BASE_DIR, '_static'),
        STATICFILES_DIRS=[os.path.join(BASE_DIR, 'static')],
        MEDIA_ROOT=os.path.join(BASE_DIR, 'data'),
        MEDIA_URL='/data/',
    )
    django.setup()
