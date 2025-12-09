# Compatibility shim to support imports like `import connection_handler` in tests
# Delegates to the canonical implementation in data_catalog.connection_handler
from data_catalog.connection_handler import *  # noqa: F401,F403
