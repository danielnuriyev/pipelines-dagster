from dagster import Definitions, job, op


@op
def noop_op():
    """An op that does nothing."""
    pass


@job
def noop_job():
    """A job that does nothing."""
    noop_op()


defs = Definitions(
    jobs=[noop_job],
)

