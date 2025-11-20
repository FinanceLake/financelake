"""
Run dash applications with a context manager.
When exiting the context, the server will close.
"""
from __future__ import print_function
import runpy
import shlex
import subprocess
import time
import uuid
import urllib
from urllib.error import URLError
import threading
import sys

import flask
import requests

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait

from pytest_dash import errors
from pytest_dash.wait_for import _wait_for_client_app_started


def _stop_server():
    stopper = flask.request.environ['werkzeug.server.shutdown']
    stopper()
    return 'stop'


def _assert_closed(driver):
    try:
        urllib.request.urlopen(driver.current_url)
    except URLError:
        return True
    return False


def _handle_error(_):
    _stop_server()


def import_app(app_file, application_name='app'):
    """
    Import a dash application from a module.
    The import path is in dot notation to the module.
    The variable named app will be returned.

    :Example:

        >>> app = import_app('my_app.app')

    Will import the application in module `app` of the package `my_app`.

    :param app_file: Path to the app (dot-separated).
    :type app_file: str
    :param application_name: The name of the dash application instance.
    :raise: pytest_dash.errors.NoAppFoundError
    :return: App from module.
    :rtype: dash.Dash
    """
    try:
        app_module = runpy.run_module(app_file)
        app = app_module[application_name]
    except KeyError:
        raise errors.NoAppFoundError(
            'No dash `app` instance was found in {}'.format(app_file)
        )
    return app


class BaseDashRunner(object):
    """Base context manager class for running applications."""

    def __init__(self, driver, keep_open=False):
        """
        :param driver: Selenium driver
        :type driver: selenium.webdriver.remote.webdriver.WebDriver
        :param keep_open: Keep the server open
        :type keep_open: bool
        """
        self.driver = driver
        self.port = 8050
        self.started = False
        self.keep_open = keep_open

    def start(self, *args, **kwargs):
        """
        Start the application.

        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError  # pragma: no cover

    def stop(self):
        """
        Stop the dash application.

        :return:
        """
        raise NotImplementedError  # pragma: no cover

    def __call__(self, *args, **kwargs):
        return self.start(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.started and not self.keep_open:
            self.stop()
            try:
                WebDriverWait(self.driver, 1).until(_assert_closed)
            except TimeoutException:  # pragma: no cover
                raise errors.ServerCloseError(
                    'Could not stop server (port={})'.format(self.port)
                )

    @property
    def url(self):
        """
        The url with the port auto-formatted.

        :return: Formatted url with the port.
        """
        return 'http://localhost:{}'.format(self.port)


class DashThreaded(BaseDashRunner):
    """Runs a dash application in a thread."""

    def __init__(self, driver, keep_open=False):
        super(DashThreaded, self).__init__(driver, keep_open=keep_open)
        self.stop_route = '/_stop-{}'.format(uuid.uuid4().hex)
        self.thread = None

    # pylint: disable=arguments-differ
    def start(
            self, app, port=8050, start_wait_time=0.5, start_timeout=10,
            **kwargs
    ):
        """
        Start the threaded dash app server.

        .. seealso:: :py:func:`~.plugin.dash_threaded`

        :param app: The dash application instance.
        :type app: dash.Dash
        :param port: Port of the dash application.
        :type port: int
        :param start_wait_time: Poll rate for the server started wait
        :type start_wait_time: float
        :param start_timeout: Max time to start the server.
        :type start_timeout: float
        :param kwargs:
        :return:
        """
        app.server.add_url_rule(self.stop_route, self.stop_route, _stop_server)
        self.port = port
        app.server.errorhandler(500)(_handle_error)

        def run():
            app.scripts.config.serve_locally = True
            app.css.config.serve_locally = True
            app.run_server(debug=False, port=port, threaded=True)

        self.thread = threading.Thread(target=run)

        self.thread.daemon = True
        self.thread.start()
        try:
            _wait_for_client_app_started(
                self.driver, self.url, start_wait_time, start_timeout
            )
        except errors.DashAppLoadingError:
            self.started = self.thread.is_alive()
            raise
        else:
            self.started = True

        return app

    def stop(self):
        requests.get('{}{}'.format(self.url, self.stop_route))
        while self.thread.is_alive():
            time.sleep(0.1)


class DashSubprocess(BaseDashRunner):
    """Runs a dash application in a waitress-serve subprocess."""

    def __init__(self, driver, keep_open=False):
        super(DashSubprocess, self).__init__(driver, keep_open=keep_open)
        self.process = None

    # pylint: disable=arguments-differ
    def start(self, app_module, application_name='app', port=8050):
        """
        Start the waitress-serve process.

        .. seealso:: :py:func:`~.plugin.dash_subprocess`

        :param app_module: Dot notation path to the app file.
        :type app_module: str
        :param application_name: Variable name of the dash instance.
        :type application_name: str
        :param port: Port to serve the application.
        :type port: int
        :return:
        """
        server_path = '{}:{}.server'.format(app_module, application_name)
        self.port = port

        is_windows = sys.platform == 'win32'

        cmd = 'waitress-serve --listen=127.0.0.1:{} {}'.format(
            port, server_path
        )
        line = shlex.split(cmd, posix=not is_windows)

        # noinspection PyTypeChecker
        self.process = subprocess.Popen(
            line, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        url = 'http://localhost:{}/'.format(port)

        try:
            _wait_for_client_app_started(self.driver, url)
        except errors.DashAppLoadingError:
            status = self.process.poll()
            print(
                '\nDash subprocess: {} Failed with status: {}'.format(
                    cmd, status
                )
            )
            self.started = status is None
            raise
        else:
            self.started = True

    def stop(self):
        self.process.kill()
        while not self.process.poll():
            time.sleep(0.01)
        out, err = self.process.communicate()
        if out:
            print(out.decode(), file=sys.stderr)  # pragma: no cover
        if err:
            print(err.decode(), file=sys.stderr)
