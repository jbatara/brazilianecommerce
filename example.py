from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

with TestPipeline() as p:
  assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))
