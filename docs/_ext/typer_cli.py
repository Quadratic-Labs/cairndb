"""A minimal Sphinx directive documenting a Typer app from its definition.

sphinx-click cannot render recent Typer apps (Typer vendors its own copy
of Click), so this introspects the command tree directly and emits one
section per subcommand with an options table:

    ```{eval-rst}
    .. typer-cli:: cairndb.cli:app
       :prog: cairndb
    ```

(from a MyST page, wrap it in ``eval-rst``: the generated markup is RST).
"""

import importlib
import inspect
from typing import ClassVar

import typer.main
from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles


def _load(ref: str):
    module, _, attr = ref.partition(":")
    return getattr(importlib.import_module(module), attr)


def _type_name(param) -> str:
    if getattr(param, "is_flag", False):
        return "flag"
    return getattr(param.type, "name", str(param.type)).lower()


def _default(param) -> str:
    if param.required:
        return "*required*"
    if getattr(param, "is_flag", False) or param.default is None:
        return ""
    return f"``{param.default}``"


def _escape(text: str) -> str:
    return text.replace("|", r"\|").replace("*", r"\*")


class TyperCliDirective(SphinxDirective):
    required_arguments = 1
    option_spec: ClassVar[dict] = {"prog": directives.unchanged}

    def run(self) -> list[nodes.Node]:
        group = typer.main.get_command(_load(self.arguments[0]))
        prog = self.options.get("prog", group.name)

        lines: list[str] = []
        if group.help:
            lines += [inspect.cleandoc(group.help), ""]
        for name, cmd in group.commands.items():
            usage = f"{prog} {name} [OPTIONS]"
            lines += [f"``{prog} {name}``", "~" * (len(prog) + len(name) + 5), ""]
            lines += [inspect.cleandoc(cmd.help or ""), ""]
            lines += [".. code-block:: text", "", f"   {usage}", ""]
            params = [p for p in cmd.params if not getattr(p, "hidden", False)]
            if params:
                lines += [
                    ".. list-table::",
                    "   :header-rows: 1",
                    "   :widths: 25 10 15 50",
                    "",
                    "   * - Option",
                    "     - Type",
                    "     - Default",
                    "     - Description",
                ]
                for p in params:
                    lines += [
                        f"   * - ``{', '.join(p.opts + p.secondary_opts)}``",
                        f"     - {_type_name(p)}",
                        f"     - {_default(p)}",
                        f"     - {_escape(p.help or '')}",
                    ]
                lines.append("")

        node = nodes.section()
        node.document = self.state.document
        nested_parse_with_titles(self.state, StringList(lines, source=self.arguments[0]), node)
        return node.children


def setup(app):
    app.add_directive("typer-cli", TyperCliDirective)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
