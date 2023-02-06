from websockets.extensions.permessage_deflate import ServerPerMessageDeflateFactory

from uvicorn.protocols.websockets import websockets_impl


class MonkeyServerPerMessageDeflateFactory(ServerPerMessageDeflateFactory):
    _per_message_deflate = None

    def process_request_params(self, params, accepted_extensions):
        if self._per_message_deflate is None:
            self._per_message_deflate = super().process_request_params(
                params, accepted_extensions
            )
        return self._per_message_deflate


def monkey():
    websockets_impl.ServerPerMessageDeflateFactory = (
        MonkeyServerPerMessageDeflateFactory
    )
    print(
        "Monkey patched uvicorn.protocols.websockets.websockets_impl.ServerPerMessageDeflateFactory"
    )
