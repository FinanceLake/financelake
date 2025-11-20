"""Experimental behavioral test api for dash apps."""
import itertools

import pytest
from ruamel import yaml

from pytest_dash import errors
from pytest_dash.application_runners import DashSubprocess
from pytest_dash.behavior_parser import parser_factory


class DashBehaviorTestFile(pytest.File):
    """A yaml test file definition"""

    def __init__(self, fspath, parent, plugin):
        super(DashBehaviorTestFile, self).__init__(fspath, parent)
        self.plugin = plugin

    def collect(self):
        raw = yaml.safe_load(self.fspath.open())
        global_application = raw.get('application')
        tests = raw.pop('Tests')
        if not tests:
            raise errors.PytestDashError(  # pragma: no cover
                'No tests defined for {}'.format(self.fspath)
            )

        for test in tests:
            kwargs = {}
            test_name = test

            if isinstance(test, str):
                behavior = raw.get(test)
            else:
                behavior_name = list(test.keys())[0]
                behavior = raw.get(behavior_name)
                kwargs = test[behavior_name]
                test_name = behavior_name + '-' + '-'.join(
                    '[{}={}]'.format(k, v) for k, v in kwargs.items()
                )

            if not behavior:
                raise errors.MissingBehaviorError(  # pragma: no cover
                    'Behavior not found: {}'.format(test_name)
                )

            yield DashBehaviorTestItem(
                self.plugin, test_name, self, behavior, global_application,
                **kwargs
            )


class DashBehaviorTestItem(pytest.Item):
    """A single test of a test file."""

    def __init__(self, plugin, name, parent, spec, application=None, **kwargs):
        super(DashBehaviorTestItem, self).__init__(name, parent)
        self._application = application or {}
        self.plugin = plugin
        self.driver = plugin.driver
        self.spec = spec
        self.parameters = kwargs

    # pylint: disable=missing-docstring
    def runtest(self):
        application = self.spec.get('application', self._application)
        app_path = application.get('path')
        app_port = application.get('port', 8050)
        app_name = application.get('application_name', 'app')
        events = self.spec.get('event')
        outcomes = self.spec.get('outcome', [])
        parameters = self.spec.get('parameters', {})
        variables = {
            k: self.parameters.get(k, v.get('default'))
            for k, v in parameters.items()
        }
        parser = parser_factory(self.driver, variables, self.plugin.behaviors)

        with DashSubprocess(self.driver) as starter:
            starter(app_path, port=app_port, application_name=app_name)
            for command in itertools.chain(events, outcomes):
                parser.parse(command)

    # pylint: disable=missing-docstring
    def reportinfo(self):
        return self.fspath, 0, "usecase: %s" % self.name
