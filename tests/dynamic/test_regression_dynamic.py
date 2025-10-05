
import pytest
from src.qaengine.driver import parse_cases, execute_case

pytestmark = pytest.mark.regression
CASES = parse_cases("regression")

@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_dynamic_regression(case):
    execute_case(case)
