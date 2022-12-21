import uvicorn
from .web import create_app
from .config import Config


def main():
    import sys
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None
    app = create_app(config_file)

    uv_config = uvicorn.Config(app, **Config.uvicorn)
    server = uvicorn.Server(uv_config)
    server.run()
