# DeprecationWarnings are silent since Python 2.7.
# The `default` filter only prints the first occurrence of matching warnings for
# each location where the warning is issued, so that we don't spam our users logs.
import warnings
warnings.simplefilter('default', DeprecationWarning)

# Export useful functions and types from private modules.
from py_zipkin.encoding._types import Encoding  # noqa
from py_zipkin.encoding._types import Kind  # noqa
