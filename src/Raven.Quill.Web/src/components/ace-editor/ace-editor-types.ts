import type { Ace } from "ace-builds";

export interface LanguageService {
    complete: (
        editor: Ace.Editor,
        session: Ace.EditSession,
        position: Ace.Point,
        prefix: string,
        callback: (errors: unknown[], completions: Ace.Completion[]) => void,
    ) => void;
    dispose: () => void;
    syntaxCheck?: (editor: Ace.Editor) => void;
}

export type AceEditorMode =
    | "csharp"
    | "css"
    | "html"
    | "javascript"
    | "json"
    | "markdown"
    | "powershell"
    | "sh"
    | "sql"
    | "text"
    | "tsx"
    | "typescript"
    | "xml"
    | "yaml"
    | (string & {});
