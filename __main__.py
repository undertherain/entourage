from dotenv import load_dotenv
from openai import OpenAI


class Agent:
    def __init__(
        self,
        model,
        system_prompt=None,
    ):
        self.model = model
        self.client = OpenAI()
        self.messages = []

    def __call__(self, prompt):
        self.messages.append(
            {
                "role": "user",
                "content": prompt,
            }
        )
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.messages,
            # tools=tools,
            # tool_choice="auto",  # auto is default, but we'll be explicit
        )
        # TODO: tream multiple choices
        self.messages.append(
            {
                "role": "assistant",
                "content": prompt,
            }
        )
        response_message = response.choices[0].message
        return response_message.content


class CLI:
    def __init__(self):
        self.agent = Agent(model="gpt-4.1-nano")

    def run(self):
        while True:
            print("> ", end="")
            try:
                user_message = input()
            except EOFError as e:
                break
            if not user_message:
                break
            res = self.agent.__call__(user_message)
            print()
            print(res)
            print()


load_dotenv()
cli = CLI()
cli.run()
# agent = Agent()
# agent()
# res = agent("whats's two plus two?")
# print(res)
