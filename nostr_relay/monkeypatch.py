try:
    from websockets.extensions.permessage_deflate import ServerPerMessageDeflateFactory

    from uvicorn.protocols.websockets import websockets_impl
except ImportError:

    def monkey():
        return None

else:

    class MonkeyServerPerMessageDeflateFactory(ServerPerMessageDeflateFactory):
        _per_message_deflate = None

        def __init__(
            self,
            server_no_context_takeover: bool = True,
            client_no_context_takeover: bool = False,
            server_max_window_bits=None,
            client_max_window_bits=None,
            compress_settings=None,
            require_client_max_window_bits: bool = False,
        ) -> None:
            super().__init__(
                server_no_context_takeover=server_no_context_takeover,
                client_no_context_takeover=client_no_context_takeover,
                server_max_window_bits=12,
                client_max_window_bits=12,
                compress_settings=compress_settings,
            )

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
