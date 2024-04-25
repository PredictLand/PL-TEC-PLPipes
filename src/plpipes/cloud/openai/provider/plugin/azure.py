from plpipes.plugin import plugin, Plugin

from openai import AzureOpenAI

@plugin
class AzureProvider(Plugin):

    @staticmethod
    def get_client(name, cfg):
        auth_args = {'api_key': cfg["api_key"],
                     'api_version': cfg.get("api_version", "2024-02-15-preview")}

        endpoint = cfg["endpoint"]
        if "deployment" in cfg:
            auth_args['base_url']=f"{endpoint}/openai/deployments/{cfg['deployment']}"
        else:
            auth_args['azure_endpoint']=endpoint

        return AzureOpenAI(**auth_args)

