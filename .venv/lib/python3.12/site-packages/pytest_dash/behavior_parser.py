"""Custom lark parser and transformer for dash behavior tests."""
import functools
import six

import lark
from selenium.webdriver.support.ui import WebDriverWait

from pytest_dash.wait_for import (
    wait_for_element_by_id, wait_for_element_by_css_selector,
    wait_for_elements_by_css_selector, wait_for_element_by_xpath,
    wait_for_elements_by_xpath
)

_grammar = r'''
start: compare
    | command

// Variable to use from the parameters
?variable: /\$[a-zA-Z0-9_]+/ -> variable

?raw_value: NUMBER -> number
    | ESCAPED_STRING -> escape_string
    | "true"i -> true_value
    | "false"i -> false_value
    | ("null"i | "none"i | "nil"i) -> null

?value: raw_value
    | element_prop
    | elements_length
    | variable
    %(value)%

?input_value: raw_value
    | variable

// Elements
element_id: /#[a-zA-Z0-9\-_]+/
element_selector: /\{.*\}/
elements_selector: /\*\{.*\}/
element: element_id | element_selector | element_xpath
element_prop: element ("." NAME)+
element_xpath: /\[.*\]/
elements_xpath: /\*\[.*\]/

elements: elements_selector | elements_xpath
elements_length: elements ".length"

// Comparisons
?eq: "should be"i | "eq" | "=="
?lt: "should be less than"i | "lt"i | "<"
?lte: "should be less or equal than"i | "lte"i | "<="
?gt: "should be greater than"i | "gt"i | ">"
?gte: "should be greater or equal than"i | "gte"i | ">="

?comparison: eq | lt | lte | gt | gte

compare: value comparison value
    | "text in" element eq value -> text_equal
    | element ("." NAME)+ comparison value -> prop_compare
    | "style" value "of" element eq value -> style_compare
    %(comparisons)%

%(custom)%

?elemental: element | elements_selector

?command: "clear" elemental -> clear
    | "click" elemental -> click
    | "enter" value "in" element -> send_value
    %(commands)%

%import common.CNAME -> NAME
%import common.NUMBER
%import common.WS
%import common.ESCAPED_STRING
%ignore WS
'''


def _compare(left, comparison, right):
    if comparison.data == 'eq':
        return left == right
    if comparison.data == 'lt':
        return left < right
    if comparison.data == 'lte':
        return left <= right
    if comparison.data == 'gt':
        return left > right
    if comparison.data == 'gte':
        return left >= right
    return False


class BehaviorTransformerMeta(type):
    """
    Dynamically create a parser transformer with user defined behaviors
    """

    # pylint: disable=too-many-locals
    def __new__(mcs, name, bases, attributes):
        behaviors = attributes.get('_behaviors', {})
        new_attrs = attributes.copy()
        behaviors_grammar = []
        values = []
        comparisons = []
        commands = []

        def wrapper(fun, inline, meta, tree):
            @functools.wraps(fun)
            @lark.v_args(inline=inline, meta=meta, tree=tree)
            # pylint: disable=unused-argument
            def _wrap(self, *args, **kwargs):
                return fun(*args, **kwargs)

            return _wrap

        for key, behavior in behaviors.items():
            new_attrs[key] = wrapper(
                behavior.handler, behavior.inline, behavior.meta, behavior.tree
            )
            if behavior.kind == 'comparison':
                # Custom comparisons need to be assigned the transformer
                # handler with the arrow inside the compare token.
                comparisons.append('{} -> {}'.format(behavior.syntax, key))
                continue
            behaviors_grammar.append('{}: {}'.format(key, behavior.syntax))
            if behavior.kind == 'value':
                values.append(key)
            elif behavior.kind == 'command':
                commands.append(key)

        grammar = _grammar
        custom_grammars = [
            ('custom', '\n'.join(behaviors_grammar)),
            ('value', '| ' + '| '.join(values) if values else ''),
            (
                'comparisons',
                '| ' + '| '.join(comparisons) if comparisons else ''
            ),
            ('commands', '|' + '| '.join(commands) if commands else ''),
        ]

        for key, value in custom_grammars:
            grammar = grammar.replace('%({})%'.format(key), value)

        new_attrs['_grammar'] = grammar

        return type.__new__(mcs, name, bases, new_attrs)


# noinspection PyMethodMayBeStatic
# pylint: disable=no-self-use, missing-docstring, no-member, R0904
@six.add_metaclass(BehaviorTransformerMeta)
@lark.v_args(inline=True)
class BehaviorTransformer(lark.Transformer, object):
    """Transform and execute behavior commands."""

    def __init__(self, driver, variables=None):
        """
        :param driver: Selenium driver to find elements in the tree
        :type driver: selenium.webdriver.remote.webdriver.WebDriver
        """
        self.driver = driver
        self.variables = variables or {}

    def variable(self, name):
        """
        A variable specified in the parameters attribute of behavior.

        :Example:

        .. code-block:: yaml

            ValueBehavior:
              parameters:
                value:
                    default: Foo
              event:
                - "enter $value in #input"
              outcome:
                - "text in #input-output should be $value"

            Tests:
                ValueBehavior
                ValueBehavior:
                    - value: Bar

        :kind: value
        """
        return self.variables.get(name.lstrip('$'))

    def number(self, num):
        return float(num) if '.' in num else int(num)

    def element_id(self, element_id):
        """
        Find an element by id when found in the tree.

        :Example: ``#dropdown``
        :kind: value
        :param element_id: Text after `#`
        """
        return wait_for_element_by_id(self.driver, element_id.replace('#', ''))

    def element_selector(self, selector):
        """
        Find an element by selector when found in the tree.

        :Example: ``{#radio-items > label:nth-child(9) > input[type="radio"]}``

        :kind: value
        :param selector: Text contained between `{` & `}`
        """
        return wait_for_element_by_css_selector(
            self.driver,
            selector.lstrip('{').rstrip('}')
        )

    def elements_selector(self, selector):
        return wait_for_elements_by_css_selector(self.driver, selector[2:-1])

    def elements(self, elements):
        return elements

    def elements_length(self, elements):
        return len(elements)

    def element(self, identifier):
        # Just need to return the element that is already found
        # by element_id or element_selector
        return identifier

    def element_prop(self, element, prop):
        """
        Property value of an element

        :Example: ``#element.prop``
        :kind: value
        """
        return element.get_property(prop)

    def element_xpath(self, xpath):
        """
        Find an element by xpath

        :Example: ``[//div/span]``
        :kind: value
        """
        return wait_for_element_by_xpath(self.driver, xpath[1:-1])

    def elements_xpath(self, xpath):
        """
        Find all elements by xpath

        :Example: ``*[//div/span]``
        :kind: value
        """
        return wait_for_elements_by_xpath(self.driver, xpath[2:-1])

    def compare(self, left, comparison, right):
        assert _compare(left, comparison, right)

    def clear(self, element):
        """
        Clear an element.

        :Example: ``clear #element``
        :kind: command
        """
        element.clear()

    def click(self, element):
        """
        Click an element.

        :Example: ``click #element``
        :kind: command
        """
        if isinstance(element, list):
            for elem in element:
                elem.click()
        else:
            element.click()

    def send_value(self, value, element):
        """
        Send key inputs to the element

        :Example: ``enter "Hello" in #input``
        :kind: command
        """
        element.send_keys(value)

    def escape_string(self, escaped):
        """
        Escaped string handler, remove the ``"`` from the token.

        :kind: value
        """
        return escaped.strip('"')

    def text_equal(self, element, _, value):
        """
        Assert the text attribute of an element is equal with a wait timer.

        :Example: ``text #output should be "Foo bar"``
        :kind: comparison
        """

        # We have the element and not the selector so we cannot use the
        # wait_for_text wrapper.
        def _text_equal(_):
            return element.text == str(value)

        WebDriverWait(self.driver, 10).until(_text_equal)

    def prop_compare(self, element, prop, comparison, value):
        """
        Wait for a property to equal a value.

        :Example: ``#output.id should be "my-element"``
        :kind: comparison
        """

        def _prop_compare(_):
            prop_value = element.get_property(prop)
            return _compare(prop_value, comparison, value)

        WebDriverWait(self.driver, 10).until(_prop_compare)

    def style_compare(self, style, element, _, value):
        """
        Compare a style value of an of element.

        :Example:

            style "color" of #style should be "rgba(0, 0, 255, 1)"

        :kind: comparison
        :param style: Name of the style property
        :param element: Element to find
        :param _: eq
        :param value: Value to compare to the element style attribute.
        :return:
        """

        def _style_compare(_):
            style_value = element.value_of_css_property(style)
            return style_value == value

        WebDriverWait(self.driver, 10).until(_style_compare)

    def true_value(self):
        return True

    def false_value(self):
        return False


def parser_factory(driver, variables=None, behaviors=None):
    """
    Create a Lark parser with a BehaviorTransformer with the provided
    selenium driver to find the elements.

    A new behavior transformer class is created and behaviors are
    assigned a transformer function from the supplied behaviors in the
    pytest_add_behaviors hook.

    :param driver: Selenium driver to use when parsing elements.
    :param variables: Variables to use in the parser transformer.
    :param behaviors: Custom behaviors, come from plugin.behaviors.
    :return:
    """

    class NewBehaviorTransformer(BehaviorTransformer):
        _behaviors = behaviors or {}

    # pylint: disable=no-member, protected-access
    # noinspection PyProtectedMember
    return lark.Lark(
        NewBehaviorTransformer._grammar,
        parser='lalr',
        transformer=NewBehaviorTransformer(driver, variables)
    )
