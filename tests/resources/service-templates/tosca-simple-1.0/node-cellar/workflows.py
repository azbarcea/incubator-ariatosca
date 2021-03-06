from aria import workflow
from aria.orchestrator.workflows.api import task
from aria.orchestrator.workflows.exceptions import TaskException


INTERFACE_NAME = 'Maintenance'
ENABLE_OPERATION_NAME = 'enable'
DISABLE_OPERATION_NAME = 'disable'


@workflow
def maintenance(ctx, graph, enabled):
    """
    Custom workflow to call the operations on the Maintenance interface.
    """

    for node in ctx.model.node.iter():
        try:
            graph.add_tasks(task.OperationTask(node,
                                               interface_name=INTERFACE_NAME,
                                               operation_name=ENABLE_OPERATION_NAME if enabled
                                               else DISABLE_OPERATION_NAME))
        except TaskException:
            pass
