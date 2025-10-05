
import pytest
from src.qaengine.driver import parse_cases, execute_case

pytestmark = pytest.mark.sanity
CASES = parse_cases("sanity")

@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_dynamic_sanity(case):
    execute_case(case)
