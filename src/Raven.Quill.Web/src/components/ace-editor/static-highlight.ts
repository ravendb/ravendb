import "ace-builds/src-noconflict/ace";
import staticHighlight from "ace-builds/src-noconflict/ext-static_highlight";
import { Mode as CSharpMode } from "ace-builds/src-noconflict/mode-csharp";
import { Mode as HtmlMode } from "ace-builds/src-noconflict/mode-html";
import { Mode as JavaScriptMode } from "ace-builds/src-noconflict/mode-javascript";
import { Mode as PowershellMode } from "ace-builds/src-noconflict/mode-powershell";
import { Mode as PythonMode } from "ace-builds/src-noconflict/mode-python";
import { Mode as ShMode } from "ace-builds/src-noconflict/mode-sh";
import { Mode as YamlMode } from "ace-builds/src-noconflict/mode-yaml";
import "@/components/ace-editor/ace-syntax-colors.css";

const MODES = {
    csharp: new CSharpMode(),
    html: new HtmlMode(),
    javascript: new JavaScriptMode(),
    powershell: new PowershellMode(),
    python: new PythonMode(),
    sh: new ShMode(),
    yaml: new YamlMode(),
};

export type HighlightLanguage = keyof typeof MODES;

// A stub theme keeps ace's own theme CSS out of the output; token colors come from
// ace-syntax-colors.css via the .ace-static-code wrapper class.
const STATIC_THEME = { cssClass: "ace-static-code", cssText: "" };

/** Renders code as syntax-highlighted HTML (escaped by ace, safe for dangerouslySetInnerHTML). */
export function highlightCode(code: string, language: HighlightLanguage): string {
    const { html } = staticHighlight.renderSync(code, MODES[language], STATIC_THEME, 1, true) as { html: string };
    return html;
}
