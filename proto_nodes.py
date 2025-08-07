import asyncio
import logging
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# send user message
# receive two tools


# # start event queue
# for CLI single-user agent it can be in-code
# in production in cab be cloud queue
# this should be configurable

# while get user message:
#     place event in a queue rather than do manually

# another option is make input a node
# and place it as a trigger

q = queue.Queue()

_nodes = {}


def node(proba=1):
    def real_decorator(function):
        name = function.__name__

        def wrapper(*args, **kwargs):
            if args:
                raise NotImplementedError(
                    "Passing parameters to nodes not implemented yet"
                )
            schedule_node(name)

        logger.info("registering node " + name)
        if name not in _nodes:
            _nodes[name] = function
            # TODO: append state ID as well
        else:
            raise ValueError("Node names must be unique")
        return wrapper

    return real_decorator


def schedule_node(name):
    q.put(name)
    print("scheduled node", name)


async def execute_node(name):
    node = _nodes[name]
    node()


@node()
def entry():
    print("this is entry point node")
    tool()


@node()
def tool():
    print("this is tool")


# TODO: make this async so that events can be added later


async def main():
    # print("Hello ...")
    # await asyncio.sleep(1)
    # print("... World!")
    while True:
        next_task = q.get()
        print("next node to execute:", next_task)
        await execute_node(next_task)


entry()
asyncio.run(main())
