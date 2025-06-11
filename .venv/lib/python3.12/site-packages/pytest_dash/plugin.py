"""
Pytest-dash plugin
------------------
Main entry point for pytest

- Hooks definitions
- Plugin config container
- Plugin selenium driver
- Fixtures
"""
import pytest

from selenium import webdriver

from pytest_dash.behaviors import DashBehaviorTestFile
from pytest_dash.errors import InvalidDriverError
from pytest_dash.application_runners import DashThreaded, DashSubprocess

_driver_map = {
    'Chrome': webdriver.Chrome,
    'Firefox': webdriver.Firefox,
    'Remote': webdriver.Remote,
    'Safari': webdriver.Safari,
    'Opera': webdriver.Opera,
    'PhantomJS': webdriver.PhantomJS,
    'Edge': webdriver.Edge,
    'Ie': webdriver.Ie,
}


def _create_config(parser, key, _help=None):
    # Create an option for pytest command line and ini
    parser.addoption('--{}'.format(key), help=_help)
    parser.addini(key, help=_help)


def _get_config(config, key, default=None):
    opt = config.getoption(key)
    ini = config.getini(key)
    return opt or ini or default


###############################################################################
# Plugin hooks.
###############################################################################


# pylint: disable=missing-docstring
def pytest_addoption(parser):
    # Add options to the pytest parser, either on the commandline or ini
    # TODO add more options for the selenium driver.
    _create_config(parser, 'webdriver', 'Name of the selenium driver to use')


# pylint: disable=too-few-public-methods
class DashPlugin(object):
    """Plugin configuration and selenium driver container"""

    def __init__(self):
        self._driver = None
        self.config = None
        self.behaviors = {}
        self._driver_name = None

    # pylint: disable=missing-docstring
    def pytest_configure(self, config):
        self.config = config
        # Called once before the tests are run
        # Get and configure global objects for the plugin to use.
        # TODO get all the options and map a global dict.
        self._driver_name = _get_config(config, 'webdriver')

        # pylint: disable=invalid-name, no-self-argument
        class _AddBehavior:
            def __init__(
                    s, syntax, kind='value', inline=True, meta=False,
                    tree=False
            ):
                s.syntax = syntax
                s.kind = kind
                s.inline = inline
                s.meta = meta
                s.tree = tree
                s.handler = None

            def __call__(s, fun):
                name = getattr(fun, '__name__')
                s.handler = fun
                self.behaviors[name] = s

        config.hook.pytest_add_behaviors(add_behavior=_AddBehavior)

    # pylint: disable=unused-argument, missing-docstring
    def pytest_unconfigure(self, config):
        # Quit the selenium driver once all tests are cleared.
        if self._driver:
            self.driver.quit()

    # pylint: disable=inconsistent-return-statements, missing-docstring
    def pytest_collect_file(self, parent, path):
        if path.ext == ".yml" and path.basename.startswith("test"):
            return DashBehaviorTestFile(path, parent, self)

    @property
    def driver(self):
        if not self._driver:
            if self._driver_name not in _driver_map:
                raise InvalidDriverError(  # pragma: no cover
                    '{} is not a valid webdriver value.\n'
                    'Valid drivers {}'.format(
                        self._driver_name, _driver_map.keys()
                    )
                )

            options = {}
            hooked_options = self.config.hook.pytest_setup_selenium(
                driver_name=self._driver_name
            ) or []
            for opt in hooked_options:
                options.update(opt)  # pragma: no cover
            self._driver = _driver_map.get(self._driver_name)(**options)

        return self._driver


_plugin = DashPlugin()


@pytest.mark.tryfirst
def pytest_addhooks(pluginmanager):
    # https://github.com/pytest-dev/pytest-xdist/blob/974bd566c599dc6a9ea291838c6f226197208b46/xdist/plugin.py#L67
    # avoid warnings with pytest-2.8
    from pytest_dash import new_hooks
    method = getattr(pluginmanager, "add_hookspecs", None)
    if method is None:
        method = pluginmanager.addhooks  # pragma: no cover
    method(new_hooks)


@pytest.mark.tryfirst
def pytest_configure(config):
    config.pluginmanager.register(_plugin)


###############################################################################
# Fixtures
###############################################################################


@pytest.fixture
def dash_threaded():
    """
    Start a local dash server in a new thread. Stop the server in teardown.

    :Example:

    .. code-block:: python

        import dash
        import dash_html_components as html

        def test_application(dash_threaded):
            app = dash.Dash(__name__)
            app.layout = html.Div('My app)
            dash_threaded(app)

    .. seealso:: :py:class:`pytest_dash.application_runners.DashThreaded`
    """

    with DashThreaded(_plugin.driver) as starter:
        yield starter


@pytest.fixture
def dash_subprocess():
    """
    Start a Dash server with subprocess.Popen and waitress-serve.

    :Example:

    .. code-block:: python

        def test_application(dash_subprocess):
            # consider the application file is named `app.py`
            dash_subprocess('app')

    .. seealso:: :py:class:`pytest_dash.application_runners.DashSubprocess`
    """
    with DashSubprocess(_plugin.driver) as starter:
        yield starter
