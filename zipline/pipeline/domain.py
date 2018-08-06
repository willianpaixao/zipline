"""
Domains
-------

TODO_SS
"""
from interface import implements, Interface
import pandas as pd

from trading_calendars import get_calendar

from zipline.country import CountryCode
from zipline.utils.input_validation import expect_types
from zipline.utils.memoize import lazyval

from .sentinels import NotSpecified


class IDomain(Interface):
    """Domain interface.
    """
    @property
    def country_code(self):
        """Country code for assets on this domain.
        """

    # TODO_SS: The original design for domains was to have them return a
    # TradingCalendar, but we have a bunch of tests in test_blaze that use very
    # short session indices that I don't know how to port to using a proper
    # TradingCalendar.
    #
    # Is there a strong reason to prefer just exposing the calendar
    # vs. exposing the sessions? If so, what do we do about the blaze tests?
    def all_sessions(self):
        """Get all trading sessions for the calendar of this domain.
        """


Domain = implements(IDomain)


# TODO: Better name for this?
# TODO: Do we want/need memoization for this?
class EquityCountryDomain(Domain):
    """TODO_SS
    """
    @expect_types(
        country_code=str,
        calendar_name=str,
        __funcname='SingleMarketDomain',
    )
    def __init__(self, country_code, calendar_name):
        self._country_code = country_code
        self._calendar_name = calendar_name

    @property
    def country_code(self):
        return self._country_code

    def all_sessions(self):
        return self.calendar.all_sessions

    @lazyval
    def calendar(self):
        return get_calendar(self._calendar_name)

    def __repr__(self):
        return "EquityCountryDomain({!r}, {!r})".format(
            self.country_code, self._calendar_name,
        )


# TODO: Is this the casing convention we want for domains?
USEquities = EquityCountryDomain(CountryCode.UNITED_STATES, 'NYSE')
CanadaEquities = EquityCountryDomain(CountryCode.CANADA, 'TSX')
# XXX: The actual country code for this is GB. Should we use that for the name
# here?
UKEquities = EquityCountryDomain(CountryCode.UNITED_KINGDOM, 'LSE')


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


class EquitySessionDomain(Domain):
    """A domain built directly from an index of sessions.

    Mostly useful for testing.

    Parameters
    ----------
    sessions : pd.DatetimeIndex
        Sessions to use as output labels for pipelines run on this domain.
    country_code : str
        ISO 3166 country code of equities to be used with this domain.
    """
    @expect_types(
        sessions=pd.DatetimeIndex,
        country_code=str,
        __funcname='EquitySessionDomain',
    )
    def __init__(self, sessions, country_code):
        self._country_code = country_code
        self._sessions = sessions

    @property
    def country_code(self):
        return self._country_code

    def all_sessions(self):
        return self._sessions


# Map from calendar name to default domain for that calendar.
_DEFAULT_ALGORITHM_DOMAINS = {
    'NYSE': USEquities,
    'TSX': CanadaEquities,
    'LSE': UKEquities,
}


def default_pipeline_domain_for_algorithm(calendar):
    """
    Get a default pipeline domain for algorithms running on ``calendar``.

    This will be used to infer a domain for pipelines that only use generic
    datasets when running in the context of a TradingAlgorithm.
    """
    return _DEFAULT_ALGORITHM_DOMAINS.get(calendar.name, NotSpecified)
