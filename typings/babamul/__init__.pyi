"""Type stubs for babamul.

Babamul does not ship a py.typed marker, so Pylance/Pyright cannot resolve
its types. These minimal stubs cover only what tom_alertstreams imports.
Remove this directory when babamul adds py.typed to its distribution.
"""

from .consumer import AlertConsumer as AlertConsumer
from .models import LsstAlert as LsstAlert, ZtfAlert as ZtfAlert
