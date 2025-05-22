import os
import importlib
from unittest.mock import patch

# Since backend.app is what we're testing, we need to be careful about when it's imported.
# It's best to import it *inside* the test functions after os.environ is patched,
# or import the module and then reload it. For module-level constants like DB_PATH,
# reloading is crucial.

# Option 1: Import module and reload inside tests
# import backend.app as app_module_to_reload

# Option 2: Import specific object (DB_PATH) inside tests after reload
# (This is tricky because DB_PATH is evaluated on first import of backend.app)

# The most reliable way for module-level constants is to ensure the module
# is loaded for the first time or reloaded *after* the patch is in effect.

def test_db_path_uses_environment_variable():
    """
    Tests if DB_PATH is set from the ALGO_DB_PATH environment variable when it's provided.
    """
    custom_path = "test/db/env_custom_path.db"
    with patch.dict(os.environ, {"ALGO_DB_PATH": custom_path}):
        # Import or reload backend.app module here to pick up the patched os.environ
        # If backend.app was already imported elsewhere (e.g. conftest or another test file),
        # simple import might not re-evaluate DB_PATH. Reloading is safer.
        import backend.app
        importlib.reload(backend.app)
        assert backend.app.DB_PATH == custom_path

def test_db_path_uses_default_when_env_var_not_set():
    """
    Tests if DB_PATH uses the default value when ALGO_DB_PATH environment variable is not set.
    """
    default_path = "database/algo_data.db"
    # Ensure ALGO_DB_PATH is not in environ. clear=True makes sure of this.
    with patch.dict(os.environ, {}, clear=True):
        import backend.app
        importlib.reload(backend.app)
        assert backend.app.DB_PATH == default_path

def test_db_path_uses_default_when_env_var_is_empty_string():
    """
    Tests if DB_PATH uses the default value if ALGO_DB_PATH is an empty string.
    os.environ.get("VAR", "default") returns "default" if VAR is "" only if the second argument to get is used to check for empty.
    However, os.environ.get() itself will return the empty string if the var is set to "".
    The code `os.environ.get("ALGO_DB_PATH", "database/algo_data.db")` means if ALGO_DB_PATH is not found OR if its value is None,
    it will use the default. If ALGO_DB_PATH is an empty string, it *is* found, so DB_PATH would be an empty string.
    This test verifies this behavior; an empty string for DB_PATH might be undesirable.
    The original subtask did not specify handling for empty string, but this is a good edge case.
    Based on `os.environ.get(KEY, default)`, if KEY exists and is an empty string, the empty string is returned.
    So, if ALGO_DB_PATH="", DB_PATH should be "".
    Let's test the actual behavior of app.py's os.environ.get()
    """
    # If ALGO_DB_PATH is set to an empty string, os.environ.get will return the empty string, not the default.
    # This test clarifies that behavior for the current implementation.
    # A more robust implementation might treat an empty string as "not set" and use the default.
    # For now, we test the current direct behavior of os.environ.get().
    
    # If the desired behavior is for empty string ALGO_DB_PATH to also fall back to default,
    # app.py would need modification like:
    # DB_PATH = os.environ.get("ALGO_DB_PATH") or "database/algo_data.db"
    # This test is for the *current* implementation:
    # DB_PATH = os.environ.get("ALGO_DB_PATH", "database/algo_data.db")
    
    custom_path_empty = ""
    default_path = "database/algo_data.db" # This is the default in os.environ.get

    with patch.dict(os.environ, {"ALGO_DB_PATH": custom_path_empty}):
        import backend.app
        importlib.reload(backend.app)
        # According to os.environ.get(KEY, default_val), if KEY exists (even if value is ""), its value is returned.
        # So, DB_PATH should be ""
        assert backend.app.DB_PATH == custom_path_empty


# To ensure tests don't interfere with each other via cached module imports of backend.app,
# it's good practice to make sure each test gets a 'fresh' view of the module after patching.
# The importlib.reload() inside each test function (after patching) is key for this.

# If we wanted to use pytest fixtures to manage the import and reload:
# import pytest

# @pytest.fixture
# def fresh_app_module():
#     import backend.app
#     importlib.reload(backend.app)
#     return backend.app

# def test_db_path_uses_environment_variable_pytest_fixture(fresh_app_module):
#     custom_path = "test/db/env_custom_path.db"
#     with patch.dict(os.environ, {"ALGO_DB_PATH": custom_path}):
#         reloaded_app = importlib.reload(fresh_app_module) # Reload again after patch
#         assert reloaded_app.DB_PATH == custom_path

# However, the simpler structure above (reloading inside test function) is also fine for this scale.
# The critical part is that backend.app.DB_PATH is resolved *after* os.environ is patched.
# Initial import of backend.app might happen before tests run if not careful.
# So, to be absolutely sure, we can also try to remove backend.app from sys.modules
# before reloading, though importlib.reload() should typically be sufficient.

# Example of more aggressive reload for test_db_path_uses_default_when_env_var_not_set:
# def test_db_path_uses_default_when_env_var_not_set_aggressive_reload():
#     default_path = "database/algo_data.db"
#     with patch.dict(os.environ, {}, clear=True):
#         if "backend.app" in sys.modules:
#             del sys.modules["backend.app"]
#         import backend.app # Import for the first time in this context or re-import
#         assert backend.app.DB_PATH == default_path
# This needs `import sys`

# For now, the existing structure with importlib.reload() should work for most test runners.
# The third test case for empty string ALGO_DB_PATH is important.
# If os.environ.get("ALGO_DB_PATH", "default") is used, and ALGO_DB_PATH="", then DB_PATH becomes "".
# If the intent is for an empty ALGO_DB_PATH to also use the default, the app.py logic should be:
# path = os.environ.get("ALGO_DB_PATH")
# DB_PATH = path if path else "database/algo_data.db"
# I will test the current implementation.
