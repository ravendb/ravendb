import { ReactNode, RefObject, useEffect, useState } from "react";
import { AceEditorMode, LanguageService } from "components/models/aceEditor";
import { Ace } from "ace-builds";
import { setCompleters } from "ace-builds/src-noconflict/ext-language_tools";
import ReactAce, { IAceEditorProps, IAceOptions, ICommand } from "react-ace";
import "./AceEditor.scss";
import classNames from "classnames";
import AceEditorContext from "./AceEditorContext";
import AceEditorFullScreenAction from "./actions/AceEditorFullScreenAction";
import AceEditorFormatAction from "./actions/AceEditorFormatAction";
import AceEditorLoadFileAction from "./actions/AceEditorLoadFileAction";
import AceEditorDeleteAction from "./actions/AceEditorDeleteAction";
import AceEditorHelpAction from "./actions/AceEditorHelpAction";
import AceEditorToggleNewLinesAction from "./actions/AceEditorToggleNewLinesAction";

interface ActionItem {
    component: ReactNode;
    position?: "top" | "bottom";
}

export interface AceEditorProps extends IAceEditorProps {
    mode: AceEditorMode;
    languageService?: LanguageService;
    validationErrorMessage?: string;
    execute?: (...args: any) => any;
    setIsValid?: (isValid: boolean) => void;
    aceRef?: RefObject<ReactAce>;
    actions?: ActionItem[];
    isFullScreenLabelHidden?: boolean;
}

function AceEditor(props: AceEditorProps) {
    const {
        aceRef,
        setOptions,
        languageService,
        validationErrorMessage,
        execute,
        setIsValid,
        actions = [],
        onLoad,
        height = "200px",
        isFullScreenLabelHidden,
        ...rest
    } = props;

    const overriddenSetOptions: IAceOptions = {
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        showLineNumbers: true,
        tabSize: 4,
        fontSize: "14px",
        showPrintMargin: false,
        ...setOptions,
    };

    const validActions = actions.filter(Boolean);

    const [aceErrorMessage, setAceErrorMessage] = useState<string>(null);

    useEffect(() => {
        if (languageService) {
            setCompleters([
                {
                    moduleId: "aceEditor",
                    getCompletions: (
                        editor: AceAjax.Editor,
                        session: AceAjax.IEditSession,
                        pos: AceAjax.Position,
                        prefix: string,
                        callback: (errors: any[], wordList: autoCompleteWordList[]) => void
                    ) => {
                        languageService.complete(editor, session, pos, prefix, callback);
                    },
                    identifierRegexps: [/[a-zA-Z_0-9'"$\-\u00A2-\uFFFF]/],
                },
            ]);
        }

        return () => languageService?.dispose();
    }, [languageService]);

    useEffect(() => {
        if (!setIsValid) {
            return;
        }

        if (aceErrorMessage) {
            setIsValid(false);
        } else {
            setIsValid(true);
        }
    }, [aceErrorMessage, setIsValid]);

    const onValidate = (annotations: Ace.Annotation[]) => {
        const firstError = annotations.find((x) => x.type === "error");

        if (firstError) {
            setAceErrorMessage(`${firstError.row},${firstError.column}: error: ${firstError.text}`);
        } else {
            setAceErrorMessage(null);
        }
    };

    const errorMessage = validationErrorMessage ?? aceErrorMessage;

    const commands: ICommand[] = execute
        ? [
              ...defaultCommands,
              {
                  name: "Execute method",
                  bindKey: {
                      win: "Ctrl+Enter",
                      mac: "Command+Enter",
                  },
                  exec: execute,
              },
          ]
        : defaultCommands;

    const handleLoad = (editor: Ace.Editor) => {
        // (ctrl+k is used for studio search)
        removeFindNextCommand(editor);

        // react-ace calls onValidate before the load and throws - Cannot read properties of null (reading 'getSession')
        // also the type 'changeAnnotation' is missing so we need to use 'as any'
        editor.getSession().on("changeAnnotation" as any, () => {
            const annotations = editor.getSession().getAnnotations();
            onValidate(annotations);
        });

        onLoad?.(editor);
    };

    return (
        <AceEditorContext.Provider value={aceRef}>
            <div className={classNames("ace-editor", { "has-error": errorMessage })}>
                <div className="react-ace-wrapper" style={{ height }}>
                    <ReactAce
                        ref={aceRef}
                        mode="csharp"
                        theme="raven"
                        editorProps={{ $blockScrolling: Infinity }}
                        fontSize={14}
                        style={{ lineHeight: "26px" }}
                        showPrintMargin={true}
                        showGutter={true}
                        highlightActiveLine={true}
                        width="100%"
                        height="100%"
                        setOptions={overriddenSetOptions}
                        commands={commands}
                        onLoad={handleLoad}
                        {...rest}
                    />
                    {actions.length > 0 && (
                        <div className="actions">
                            <div className="d-flex flex-column h-100">
                                <div className="flex-grow-0 vstack gap-1">
                                    {validActions
                                        .filter((action) => !action.position || action.position === "top")
                                        .map((action, index) => (
                                            <div key={index}>{action.component}</div>
                                        ))}
                                </div>
                                <div className="flex-grow-1 d-flex flex-column justify-content-end vstack gap-1">
                                    {validActions
                                        .filter((icon) => icon.position === "bottom")
                                        .map((action, index) => (
                                            <div key={index}>{action.component}</div>
                                        ))}
                                </div>
                            </div>
                        </div>
                    )}
                    {!isFullScreenLabelHidden && (
                        <span className="fullScreenModeLabel">Press Shift+F11 to enter full screen mode</span>
                    )}
                </div>
                {errorMessage && (
                    <div className="bg-faded-danger py-1 px-2">
                        <small>{errorMessage}</small>
                    </div>
                )}
            </div>
        </AceEditorContext.Provider>
    );
}

const defaultCommands: ICommand[] = [
    {
        name: "Open Fullscreen",
        bindKey: {
            win: "Shift+F11",
            mac: "Shift+F11",
        },
        exec: function (editor: Ace.Editor) {
            editor.container.requestFullscreen();
        },
        readOnly: true,
    },
];

const removeFindNextCommand = (editor: Ace.Editor) => {
    editor.commands.removeCommand(editor.commands.byName.findnext);
};

AceEditor.FullScreenAction = AceEditorFullScreenAction;
AceEditor.FormatAction = AceEditorFormatAction;
AceEditor.LoadFileAction = AceEditorLoadFileAction;
AceEditor.DeleteAction = AceEditorDeleteAction;
AceEditor.HelpAction = AceEditorHelpAction;
AceEditor.ToggleNewLinesAction = AceEditorToggleNewLinesAction;

export default AceEditor;
