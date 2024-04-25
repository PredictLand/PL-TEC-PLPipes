from plpipes.plugin import plugin, Plugin

from openai import OpenAI

@plugin
class OpenAIProvider(Plugin):

    @staticmethod
    def get_client(name, cfg):
        api_key = cfg["api_key"]
        return OpenAI(api_key = api_key)

