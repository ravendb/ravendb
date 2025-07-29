import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { editAiAgentActions, editAiAgentSelectors } from "../store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useRef, useEffect } from "react";
import _ from "lodash";
import AiAgentMessages from "../../partials/AiAgentMessages";
import AiAgentParametersField from "../../partials/AiAgentParametersField";
import { editAiAgentUtils } from "../utils/editAiAgentUtils";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import SizeGetter from "components/common/SizeGetter";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";

export default function EditAiAgentTestPanel() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, setValue } = useFormContext<EditAiAgentFormData>();
    const rawDataRef = useRef<ReactAce>(null);

    const formValues = useWatch({
        control,
    });

    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);
    const testDocument = useAppSelector(editAiAgentSelectors.testDocument);
    const isRawData = useAppSelector(editAiAgentSelectors.isRawData);
    const messages = useAppSelector(editAiAgentSelectors.testMessages);
    const runTestState = useAppSelector(editAiAgentSelectors.runTestState);
    const isWaitingForActionToolSubmit = useAppSelector(editAiAgentSelectors.isWaitingForActionToolSubmit);

    const isLoading = runTestState === "loading" || isWaitingForActionToolSubmit;

    const runTest = async (toolCallParameters?: AiAgentToolCall[]) => {
        await dispatch(editAiAgentActions.runTest({ databaseName, formValues, toolCallParameters })).unwrap();
        setValue("test.prompt", "");
    };

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    // Scroll to the bottom of the test panel when new messages are added
    useEffect(() => {
        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    const { Actions, Queries } = editAiAgentUtils.mapToDto(formValues);

    const handleNewChat = () => {
        dispatch(editAiAgentActions.testDocumentSet(null));
        dispatch(editAiAgentActions.testMessagesSet([]));
        dispatch(editAiAgentActions.isWaitingForActionToolSubmitSet(false));
        setValue("test.prompt", "");
    };

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary hstack justify-content-between">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test agent
                </h3>
                {isTestOpen && (
                    <div className="hstack gap-2">
                        {messages.length > 0 && (
                            <Button
                                variant="primary"
                                size="sm"
                                onClick={handleNewChat}
                                className="rounded-pill"
                                title="Clear the current conversation and start a new chat"
                            >
                                New chat
                            </Button>
                        )}
                        {testDocument && messages.length > 0 && (
                            <Button
                                variant="secondary"
                                size="sm"
                                onClick={() => dispatch(editAiAgentActions.isRawDataSet(!isRawData))}
                                className="rounded-pill"
                                title="Switch between chat and raw data display"
                            >
                                <Icon icon={isRawData ? "ai-agents" : "json"} margin="m-0" />
                            </Button>
                        )}
                        <Button
                            variant="secondary"
                            size="sm"
                            onClick={() => dispatch(editAiAgentActions.isTestOpenSet(false))}
                            className="rounded-pill"
                        >
                            <Icon icon="close" /> Close
                        </Button>
                    </div>
                )}
            </div>
            <div className="w-100 flex-grow-1 vstack justify-content-center align-items-center overflow-auto">
                <div className="flex-grow-1 vstack w-100 overflow-auto p-2 position-relative" ref={messagesPanelRef}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center">
                            <AiAgentParametersField
                                control={control}
                                name="test.parameters"
                                value={formValues.test.parameters}
                            />
                        </div>
                    )}
                    {!isRawData && messages.length > 0 && (
                        <AiAgentMessages
                            messages={messages}
                            toolQueries={Queries}
                            toolActions={Actions}
                            handleSaveParameters={(toolCallParameters) => runTest(toolCallParameters)}
                            setIsWaitingForActionToolSubmit={(value: boolean) =>
                                dispatch(editAiAgentActions.isWaitingForActionToolSubmitSet(value))
                            }
                        />
                    )}
                    {isRawData && testDocument && (
                        <SizeGetter
                            isHeighRequired
                            render={({ height }) => (
                                <AceEditor
                                    aceRef={rawDataRef}
                                    mode="json"
                                    value={JSON.stringify(testDocument, null, 2)}
                                    height={`${height}px`}
                                    readOnly
                                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                                />
                            )}
                        />
                    )}
                    {runTestState === "loading" && (
                        <div className="position-absolute top-50 start-50 translate-middle">
                            <Spinner animation="border" />
                        </div>
                    )}
                </div>
                <div className="w-100 p-2 panel-bg-2 border-top border-secondary">
                    <div className="position-relative">
                        <FormInput
                            type="textarea"
                            as="textarea"
                            control={control}
                            name="test.prompt"
                            placeholder="Ask the agent anything"
                            className="rounded-2"
                            style={{ resize: "none" }}
                            disabled={isLoading}
                            onKeyDown={(e) => {
                                if (e.key === "Enter" && !e.shiftKey) {
                                    e.preventDefault();
                                    runTest();
                                }
                            }}
                        />
                        {formValues.test.prompt && (
                            <ButtonWithSpinner
                                variant="secondary"
                                icon="arrow-up"
                                onClick={() => runTest()}
                                isSpinning={isLoading}
                                className="position-absolute rounded-pill"
                                style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                            />
                        )}
                    </div>
                </div>
            </div>
        </>
    );
}
