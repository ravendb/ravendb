import { useEffect, useRef, useState, type ReactNode, type RefObject } from "react";
import type { Ace } from "ace-builds";
import "ace-builds/src-noconflict/ace";
import { setCompleters } from "ace-builds/src-noconflict/ext-language_tools";
import ReactAce, { type IAceEditorProps as ReactAceEditorProps, type IAceOptions } from "react-ace";
import "ace-builds/src-noconflict/mode-csharp";
import "ace-builds/src-noconflict/mode-css";
import "ace-builds/src-noconflict/mode-html";
import "ace-builds/src-noconflict/mode-javascript";
import "ace-builds/src-noconflict/mode-json";
import "ace-builds/src-noconflict/mode-markdown";
import "ace-builds/src-noconflict/mode-powershell";
import "ace-builds/src-noconflict/mode-sh";
import "ace-builds/src-noconflict/mode-sql";
import "ace-builds/src-noconflict/mode-text";
import "ace-builds/src-noconflict/mode-tsx";
import "ace-builds/src-noconflict/mode-typescript";
import "ace-builds/src-noconflict/mode-xml";
import "ace-builds/src-noconflict/mode-yaml";
import "ace-builds/src-noconflict/theme-textmate";
import "ace-builds/src-noconflict/ext-searchbox";
import "ace-builds/src-noconflict/ext-language_tools";
import {
    ACE_EDITOR_LINE_HEIGHT_IN_PX,
    ACE_EDITOR_MAX_HEIGHT_IN_PX,
    ACE_EDITOR_MIN_HEIGHT_IN_PX,
} from "@/components/ace-editor/ace-editor-constants";
import AceEditorContext from "@/components/ace-editor/ace-editor-context";
import {
    AceEditorAutoResizeHeightAction,
    AceEditorDeleteAction,
    AceEditorFormatAction,
    AceEditorFullScreenAction,
    AceEditorHelpAction,
    AceEditorLoadFileAction,
} from "@/components/ace-editor/ace-editor-actions";
import { handleAutoResizeHeight } from "@/components/ace-editor/ace-editor-action-utils";
import type { AceEditorMode, LanguageService } from "@/components/ace-editor/ace-editor-types";
import { useResizableHeight } from "@/components/ace-editor/use-resizable-height";
import { cn } from "@/lib/utils";
import "./ace-editor.css";

// Vite 8's Rolldown bundler uses Node-style CommonJS interop, so the default import of
// react-ace is the whole module.exports object and the actual component sits on `.default`.
// Unwrap it for rendering, falling back to the import itself for bundlers that already
// unwrap (e.g. the production build). The `ReactAce` default import is still used directly
// as a type for the editor refs below.
const ReactAceComponent = ((ReactAce as { default?: typeof ReactAce }).default ?? ReactAce) as typeof ReactAce;

type ActionItem = {
    component: ReactNode;
    position?: "bottom" | "top";
};

export type AceEditorProps = Omit<ReactAceEditorProps, "mode"> & {
    aceRef?: RefObject<ReactAce | null>;
    actions?: ActionItem[];
    execute?: (...args: unknown[]) => unknown;
    isFillHeight?: boolean;
    languageService?: LanguageService;
    maxHeight?: number | string;
    minHeight?: number | string;
    mode: AceEditorMode;
    setIsValid?: (isValid: boolean) => void;
    validationErrorMessage?: string;
};

function toHeightNumber(height: number | string, fallback: number) {
    if (typeof height === "number") {
        return height;
    }

    if (typeof height === "string") {
        const parsed = Number.parseFloat(height);

        if (Number.isFinite(parsed)) {
            return parsed;
        }
    }

    return fallback;
}

function removeFindNextCommand(editor: Ace.Editor) {
    const commands = editor.commands as Ace.Editor["commands"] & {
        byName?: Record<string, unknown>;
        removeCommand?: (command: unknown) => void;
    };
    const findNextCommand = commands.byName?.findnext;

    if (findNextCommand) {
        commands.removeCommand?.(findNextCommand);
    }
}

function AceEditor({
    aceRef,
    actions = [],
    className,
    height = "200px",
    isFillHeight = false,
    languageService,
    maxHeight = ACE_EDITOR_MAX_HEIGHT_IN_PX,
    minHeight = ACE_EDITOR_MIN_HEIGHT_IN_PX,
    mode,
    onLoad,
    onValidate,
    setIsValid,
    setOptions,
    validationErrorMessage,
    ...props
}: AceEditorProps) {
    const defaultAceRef = useRef<ReactAce | null>(null);
    const editorRef = aceRef ?? defaultAceRef;
    const rootRef = useRef<HTMLDivElement | null>(null);
    const [aceErrorMessage, setAceErrorMessage] = useState<string>();
    const resizableHeight = useResizableHeight({
        initialHeight: toHeightNumber(height, 200),
        maxHeight: toHeightNumber(maxHeight, ACE_EDITOR_MAX_HEIGHT_IN_PX),
        minHeight: toHeightNumber(minHeight, ACE_EDITOR_MIN_HEIGHT_IN_PX),
    });

    const errorMessage = validationErrorMessage ?? aceErrorMessage;
    const validActions = actions.filter(Boolean);
    const topActions = validActions.filter((action) => !action.position || action.position === "top");
    const bottomActions = validActions.filter((action) => action.position === "bottom");
    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        fontFamily: "var(--font-mono)",
        fontSize: "14px",
        showLineNumbers: true,
        showPrintMargin: false,
        tabSize: 4,
        ...setOptions,
    };

    useEffect(() => {
        if (!languageService) {
            return;
        }

        setCompleters([
            {
                getCompletions: (editor, session, position, prefix, callback) => {
                    languageService.complete(editor, session, position, prefix, callback);
                },
                identifierRegexps: [/[a-zA-Z_0-9'"$\-\u00A2-\uFFFF]/],
            },
        ]);

        return () => languageService.dispose();
    }, [languageService]);

    useEffect(() => {
        setIsValid?.(!errorMessage);
    }, [errorMessage, setIsValid]);

    useEffect(() => {
        editorRef.current?.editor.resize();
    }, [editorRef, resizableHeight.height]);

    // In fill mode the body is sized by flex layout, so Ace must re-measure on container resizes.
    useEffect(() => {
        const root = rootRef.current;

        if (!isFillHeight || !root) {
            return;
        }

        const observer = new ResizeObserver(() => editorRef.current?.editor.resize());

        observer.observe(root);

        return () => observer.disconnect();
    }, [isFillHeight, editorRef]);

    function handleValidate(annotations: Parameters<NonNullable<ReactAceEditorProps["onValidate"]>>[0]) {
        const firstError = annotations.find((annotation) => annotation.type === "error");

        setAceErrorMessage(
            firstError ? `${firstError.row},${firstError.column}: error: ${firstError.text}` : undefined,
        );
        onValidate?.(annotations);
    }

    function handleLoad(editorProps: unknown) {
        const editor = editorProps as Ace.Editor;

        removeFindNextCommand(editor);
        languageService?.syntaxCheck?.(editor);
        onLoad?.(editorProps as never);
    }

    return (
        <AceEditorContext.Provider value={{ aceRef: editorRef, rootRef, setHeight: resizableHeight.setHeight }}>
            <div
                className={cn("ace-editor", isFillHeight && "ace-editor--fill", className)}
                data-dragging={resizableHeight.isDragging}
                data-invalid={Boolean(errorMessage)}
                ref={rootRef}
            >
                <div
                    className="ace-editor__body"
                    style={isFillHeight ? undefined : { height: `${resizableHeight.height}px` }}
                >
                    <ReactAceComponent
                        editorProps={{ $blockScrolling: Infinity }}
                        fontSize={14}
                        height="100%"
                        highlightActiveLine
                        mode={mode}
                        onLoad={handleLoad}
                        onValidate={handleValidate}
                        ref={editorRef}
                        setOptions={overriddenSetOptions}
                        showGutter
                        showPrintMargin={false}
                        style={{ lineHeight: `${ACE_EDITOR_LINE_HEIGHT_IN_PX}px` }}
                        theme="textmate"
                        width="100%"
                        {...props}
                    />
                    {validActions.length > 0 && (
                        <div className="ace-editor__actions">
                            <div className="ace-editor__actions-group">
                                {topActions.map((action, index) => (
                                    <div key={index}>{action.component}</div>
                                ))}
                            </div>
                            <div className="ace-editor__actions-group">
                                {bottomActions.map((action, index) => (
                                    <div key={index}>{action.component}</div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
                {errorMessage && <div className="ace-editor__error">{errorMessage}</div>}
                {!isFillHeight && (
                    <div
                        className="ace-editor__resize-handle"
                        onDoubleClick={() => handleAutoResizeHeight(editorRef, resizableHeight.setHeight)}
                        onMouseDown={resizableHeight.handleMouseDown}
                    />
                )}
            </div>
        </AceEditorContext.Provider>
    );
}

AceEditor.FullScreenAction = AceEditorFullScreenAction;
AceEditor.FormatAction = AceEditorFormatAction;
AceEditor.LoadFileAction = AceEditorLoadFileAction;
AceEditor.DeleteAction = AceEditorDeleteAction;
AceEditor.HelpAction = AceEditorHelpAction;
AceEditor.AutoResizeHeightAction = AceEditorAutoResizeHeightAction;

export type { AceEditorMode, LanguageService } from "@/components/ace-editor/ace-editor-types";
export default AceEditor;
