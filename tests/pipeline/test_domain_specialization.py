import pandas as pd

from zipline.pipeline import Pipeline
from zipline.pipeline.data import Column, DataSet
from zipline.pipeline.data.testing import TestingDataSet
from zipline.pipeline.domain import (
    CanadaEquities,
    UKEquities,
    USEquities,
)
from zipline.pipeline.factors import CustomFactor
import zipline.testing.fixtures as zf
from zipline.testing.core import parameter_space, powerset
from zipline.testing.predicates import assert_equal


class Sum(CustomFactor):

    def compute(self, today, assets, out, data):
        out[:] = data.sum(axis=0)

    @classmethod
    def create(cls, column, window_length):
        return cls(inputs=[column], window_length=window_length)


class MixedGenericsTestCase(zf.WithSeededRandomPipelineEngine,
                            zf.ZiplineTestCase):
    START_DATE = pd.Timestamp('2014-01-02', tz='utc')
    END_DATE = pd.Timestamp('2014-01-31', tz='utc')
    ASSET_FINDER_EQUITY_SIDS = (1, 2, 3, 4, 5)
    ASSET_FINDER_COUNTRY_CODE = 'US'

    def test_mixed_generics(self):
        """
        Test that we can run pipelines with mixed generic/non-generic terms.

        This test is a regression test for failures encountered during
        development where having a mix of generic and non-generic columns in
        the term graph caused bugs in our extra row accounting.
        """
        USTestingDataSet = TestingDataSet.specialize(USEquities)
        base_terms = {
            'sum3_generic': Sum.create(TestingDataSet.float_col, 3),
            'sum3_special': Sum.create(USTestingDataSet.float_col, 3),
            'sum10_generic': Sum.create(TestingDataSet.float_col, 10),
            'sum10_special': Sum.create(USTestingDataSet.float_col, 10),
        }

        def run(ts):
            pipe = Pipeline(ts, domain=USEquities)
            start = self.trading_days[-5]
            end = self.trading_days[-1]
            return self.run_pipeline(pipe, start, end)

        base_result = run(base_terms)

        for subset in powerset(base_terms):
            subset_terms = {t: base_terms[t] for t in subset}
            result = run(subset_terms).sort_index(axis=1)
            expected = base_result[list(subset)].sort_index(axis=1)
            assert_equal(result, expected)


class SpecializeTestCase(zf.ZiplineTestCase):

    @parameter_space(domain=[USEquities, CanadaEquities, UKEquities])
    def test_specialize(self, domain):
        class MyDataSet(DataSet):
            col1 = Column(dtype=float)
            col2 = Column(dtype=int, missing_value=100)
            col3 = Column(dtype=object, missing_value="")

        specialized = MyDataSet.specialize(domain)

        # Specializations should be memoized.
        self.assertIs(specialized, MyDataSet.specialize(domain))

        # Specializations should have the same name.
        assert_equal(specialized.__name__, "MyDataSet")
        self.assertIs(specialized.domain, domain)

        for attr in ('col1', 'col2', 'col3'):
            original = getattr(MyDataSet, attr)
            new = getattr(specialized, attr)

            # We should get a new column from the specialization, which should
            # be the same object that we would get from specializing the
            # original column.
            self.assertIsNot(original, new)
            self.assertIs(new, original.specialize(domain))

            # Columns should be bound to their respective datasets.
            self.assertIs(original.dataset, MyDataSet)
            self.assertIs(new.dataset, specialized)

            # The new column should have the domain of the specialization.
            assert_equal(new.domain, domain)

            # Names, dtypes, and missing_values should match.
            assert_equal(original.name, new.name)
            assert_equal(original.dtype, new.dtype)
            assert_equal(original.missing_value, new.missing_value)

    @parameter_space(domain=[USEquities, CanadaEquities, UKEquities])
    def test_unspecialize(self, domain):

        class MyDataSet(DataSet):
            col1 = Column(dtype=float)
            col2 = Column(dtype=int, missing_value=100)
            col3 = Column(dtype=object, missing_value="")

        specialized = MyDataSet.specialize(domain)
        unspecialized = specialized.unspecialize()
        specialized_again = unspecialized.specialize(domain)

        self.assertIs(unspecialized, MyDataSet)
        self.assertIs(specialized, specialized_again)

        for attr in ('col1', 'col2', 'col3'):
            original = getattr(MyDataSet, attr)
            new = getattr(specialized, attr)
            # Unspecializing a specialization should give back the original.
            self.assertIs(new.unspecialize(), original)
            # Specializing again should give back the same as the first
            # specialization.
            self.assertIs(new.unspecialize().specialize(domain), new)
