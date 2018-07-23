"""
Domains
-------

TODO_SS
"""
from trading_calendars import get_calendar

from zipline.country import CountryCode

from .sentinels import NotSpecified


# TODO: Memoize these?
class Domain(object):
    """TODO_SS
    """
    def __init__(self, name, country_code, calendar_name):
        self._name = name
        self._country_code = country_code
        self._calendar_name = calendar_name

    @property
    def name(self):
        return self._name

    @property
    def country_code(self):
        return self._country_code

    @property
    def calendar_name(self):
        return self._calendar_name

    def get_calendar(self):
        return get_calendar(self.calendar_name)

    def __repr__(self):
        return "{}(country={!r}, calendar={!r})".format(
            self.name,
            self.country_code,
            self.calendar_name,
        )


# TODO: Is this the casing convention we want for domains?
USEquities = Domain('USEquities', CountryCode.UNITED_STATES, 'NYSE')
CanadaEquities = Domain('CanadaEquities', CountryCode.CANADA, 'TSX')
# XXX: The actual country code for this is GB. Should we use that for the name
# here?
UKEquities = Domain('UKEquities', CountryCode.UNITED_KINGDOM, 'LSE')


def infer_domain(terms):
    """
    Infer the domain from a collection of terms.

    The algorithm for inferring domains is as follows:

    - If all input terms have a domain of NotSpecified, the result is
      NotSpecified.

    - If there is exactly one non-NotSpecified domain in the input terms, the
      result is that domain.

    - Otherwise, an AmbiguousDomain error is raised.

    Parameters
    ----------
    terms : iterable[zipline.pipeline.term.Term]

    Returns
    -------
    inferred : Domain or NotSpecified

    Raises
    ------
    AmbiguousDomain
        Raised if more than one concrete domain is present in the input terms.
    """
    domains = {NotSpecified}
    for t in terms:
        domains.add(t.domain)

    if len(domains) == 1:
        return NotSpecified
    elif len(domains) == 2:
        domains.remove(NotSpecified)
        return domains.pop()
    else:
        domains.remove(NotSpecified)
        raise AmbiguousDomain(sorted(domains, key=lambda d: d.country_code))


class AmbiguousDomain(Exception):
    """
    Raised when we attempt to infer a domain from a collection of mixed terms.
    """
