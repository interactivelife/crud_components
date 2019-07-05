import abc


class SummaryMixin:
    @property
    @abc.abstractmethod
    def summary_text(self):
        return '<N/A>'

    @property
    def summary_subtext(self):
        return ''
