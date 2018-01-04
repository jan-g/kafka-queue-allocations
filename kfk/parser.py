import argparse
import argcomplete
import logging
import logging.config

LOG = logging.getLogger(__name__)


def construct_parser(commands=None):
    parser = argparse.ArgumentParser()
    parsers = {
        "": (parser, None),
    }
    for c in commands:
        if len(c) == 2:
            line, func = c
            kwargs = {}
        else:
            line, func, kwargs = c
        words = line.split()
        thus_far = []
        key = ''
        for word in words:
            if word.startswith('-'):
                parsers[key][0].add_argument(word, **kwargs)
                continue
            elif word.startswith('='):
                parsers[key][0].add_argument(word[1:], **kwargs)
                continue
            elif word.startswith('?'):
                parsers[key][0].add_argument(word[1:], action='store_true', **kwargs)
                continue
            elif word.startswith('&'):
                parsers[key][0].add_argument(word[1:], action='append', **kwargs)
                continue
            elif word.startswith('+'):
                parsers[key][0].add_argument(word[1:], nargs="+", **kwargs)
                continue
            elif word.startswith('*'):
                parsers[key][0].add_argument(word[1:], nargs="*", **kwargs)
                continue
            elif word.startswith('!'):
                parsers[key][0].add_argument(word[1:], required=True, **kwargs)
                continue
            parent_parser, parent_sp = parsers[key]
            thus_far.append(word)
            pkey = key
            key = ' '.join(thus_far)
            if key in parsers:
                continue
            if parent_sp is None:
                parent_sp = parent_parser.add_subparsers()
                parsers[pkey] = (parent_parser, parent_sp)
            sub_parser = parent_sp.add_parser(word, **kwargs)
            parsers[key] = (sub_parser, None)
        if func is not None:
            parsers[key][0].set_defaults(func=func)
    return parser


def setup_debug(debug):
    """
    config = {
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
            },
        },
        "root": {
            "level": logging.WARN,
            "handlers": ["console"],
        },
        "loggers": {
            "kfk": {
                "level": logging.DEBUG,
            },
            "requests": {
                "level": logging.WARN,
            },
            "kafka": {
                "level": [logging.WARN, logging.DEBUG][debug]
            }
        },
    }
    logging.config.dictConfig(config)
    """
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('kafka').setLevel([logging.WARN, logging.DEBUG][debug - 1])


def main(commands=None):
    DEBUG = ("--debug", None, {"action": "count"})
    if DEBUG not in commands:
        commands.append(DEBUG)
    parser = construct_parser(commands)
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.debug is not None:
        setup_debug(args.debug)
    LOG.debug(args)
    if getattr(args, 'prefix', None) is None:
        args.prefix = ''
    args.func(args)
