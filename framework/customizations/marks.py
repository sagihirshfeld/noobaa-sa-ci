import pytest

# tier1 - basic functionality testing of core features
tier1 = pytest.mark.tier1(value=1)

# tier2 - testing core features with the addition of complexity
tier2 = pytest.mark.tier2(value=2)

# tier3 - edge cases and negative testing
tier3 = pytest.mark.tier3(value=3)

# tier4 - disruptive testing
tier4 = pytest.mark.tier4(value=4)

# build acceptance critieria
acceptance = pytest.mark.acceptance
