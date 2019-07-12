import abc


class AbstractMetadataBuilder(abc.ABC):

    @property
    @abc.abstractmethod
    def metadata_cls(self):
        raise NotImplementedError()

    def build(self, attr_key, attr, field_info: dict):
        processed_info = self._process_info(attr_key, attr, field_info)
        return self.metadata_cls(**processed_info)

    @abc.abstractmethod
    def _process_info(self, attr_key, attr, field_info):
        raise NotImplementedError()
