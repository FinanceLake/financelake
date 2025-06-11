"""Custom hooks for pytest dash"""

# pylint: disable=unused-argument


def pytest_add_behaviors(add_behavior):
    """
    Use this hook to add custom behavior parsing.

    **Example**
    `conftest.py`

    .. code-block:: python

        def pytest_add_behavior(add_behavior):
            @add_behavior('Text to parse')
            def custom_parse_action(params):
                pass

    :param add_behavior: Decorator for a behavior handler function.
    :return:
    """


def pytest_setup_selenium(driver_name):
    """
    Called before the driver is created, return a dictionary to use as kwargs
    for the driver init.

    :param driver_name: The name of the driver specified by either cli
        argument or in pytest.ini.
    :type driver_name: str
    :return: The dictionary of kwargs to give to the driver constructor.
    """
