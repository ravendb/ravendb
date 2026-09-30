/**
 * Template literal tag that strips the common leading indentation from every line,
 * so multi-line strings (sample scripts, prompts) can be indented to match the surrounding code.
 *
 * The first line (right after the opening backtick) and the last line (before the closing backtick)
 * are dropped when they contain only whitespace.
 */
export function dedent(strings: TemplateStringsArray, ...values: unknown[]): string {
    const text = strings.reduce((result, part, i) => result + part + (i < values.length ? String(values[i]) : ""), "");

    const lines = text.split("\n");

    if (lines.length > 0 && lines[0].trim() === "") {
        lines.shift();
    }
    if (lines.length > 0 && lines[lines.length - 1].trim() === "") {
        lines.pop();
    }

    const indents = lines.filter((line) => line.trim() !== "").map((line) => line.match(/^[ \t]*/)[0].length);
    const minIndent = indents.length > 0 ? Math.min(...indents) : 0;

    return lines.map((line) => line.slice(minIndent)).join("\n");
}
