from ..crud_hook import CrudHook
from .....integration.kc import kc_transaction_stack, kc_command_stack


class KcCommandHook(CrudHook):

    def on_success(self):
        kc_command_stack.delete_stack()
        super().on_success()

    def on_failure(self, exception):
        kc_transaction_stack.push(kc_command_stack.list_key)
        super().on_failure(exception)

