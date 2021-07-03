class MessageCodes(object):
    def __init__(self):
        self.message_codes = {
            'unknown': 'unknown action',
            'message': '{0}',
            'requests_408': 'request timeout={0} for url="{1}"',
            'requests_500': 'request error for url="{0}": {1}',
            'requests_not_200': 'status_code={0}, bad request response for url="{1}": {2}',
            'bad_input_params_type': 'function="{0}" got parameter "{1}" with type="{3}" whilst expected type is "{2}"',
            'bad_response_no_attribute': 'function="{0}" got bad response from external service: attribute="{1}" is not found',
            'bad_response_type': 'function="{0}" got bad response from external service: attribute="{1}" is {2}',
            'external_service_unavailable': 'function="{0}" has no response from external service',
            'spark_session_failed': 'failed to create Spark session: {0}',
            'spark_context_failed': 'failed to create Spark context: {0}',
            'spark_sql_context_failed': 'failed to create Spark SQL context: {0}',
            'pipeline_validation_fail': 'job_name={0}: validation failed for the pipeline "{1}"',
            'pipeline_job_start': 'job_name={0}: started at {1}',
            'pipeline_job_finish': 'job_name={0}: finished at {1}',
            'pipeline_job_not_finished': 'job_name={0}: not finished properly: {1}',
        }

    def get_error_message(self, code: str, *args):
        msg = self.message_codes.get(code, '')
        if msg:
            formatted_msg = format(msg.format(*args))
            return f'error "{code}": {formatted_msg}'

        formatted_msg = self.message_codes['unknown']
        return f'error "unknown": {formatted_msg}'

    def get_info_message(self, code: str, *args):
        msg = self.message_codes.get(code, '')
        if msg:
            formatted_msg = format(msg.format(*args))
            return f'info "{code}": {formatted_msg}'

        formatted_msg = self.message_codes['unknown']
        return f'info "unknown": {formatted_msg}'
