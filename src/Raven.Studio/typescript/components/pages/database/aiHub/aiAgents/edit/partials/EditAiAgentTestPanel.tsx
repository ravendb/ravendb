import { Icon } from "components/common/Icon";
import { UseFormReturn, useWatch } from "react-hook-form";
import { EditAiAgentFormData, TestAiAgentFormData } from "../utils/editAiAgentValidation";
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
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import SizeGetter from "components/common/SizeGetter";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import { tryHandleSubmit } from "components/utils/common";
import messagePublisher from "common/messagePublisher";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { compareSets } from "common/typeUtils";

interface EditAiAgentTestPanelProps {
    testForm: UseFormReturn<TestAiAgentFormData>;
    editForm: UseFormReturn<EditAiAgentFormData>;
    allQueriesNames: string[];
}

export default function EditAiAgentTestPanel({ testForm, editForm, allQueriesNames }: EditAiAgentTestPanelProps) {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const rawDataRef = useRef<ReactAce>(null);

    const testFormValues = useWatch({
        control: testForm.control,
    });
    const editFormValues = useWatch({
        control: editForm.control,
    });

    const testDocument = useAppSelector(editAiAgentSelectors.testDocument);
    const isRawData = useAppSelector(editAiAgentSelectors.isRawData);
    const messages = useAppSelector(editAiAgentSelectors.testMessages);
    const runTestState = useAppSelector(editAiAgentSelectors.runTestState);
    const isWaitingForActionToolSubmit = useAppSelector(editAiAgentSelectors.isWaitingForActionToolSubmit);

    const hasLatestParameters = compareSets(
        editFormValues.parameters?.map((x) => x.name) ?? [],
        testFormValues.parameters?.map((x) => x.name) ?? []
    );

    const hasMissingParameters = messages.length > 0 && testFormValues.parameters.some((x) => !x.value);

    const isLoading = runTestState === "loading" || isWaitingForActionToolSubmit;
    const isTestDisabled = !hasLatestParameters || hasMissingParameters || isLoading;

    const configuration = editAiAgentUtils.mapToDto(editFormValues);

    const runTest = async (toolCallParameters?: AiAgentToolCall[]) => {
        return tryHandleSubmit(async () => {
            await dispatch(
                editAiAgentActions.runTest({
                    databaseName,
                    configuration,
                    testFormValues,
                    toolCallParameters,
                    allQueriesNames,
                })
            ).unwrap();

            testForm.setValue("prompt", "");
        });
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            runTest();
        });
    };

    const handleSaveParameters = async (toolCallParameters?: AiAgentToolCall[]) => {
        if (!hasLatestParameters) {
            messagePublisher.reportError(parametersNotUpToDateText);
            throw new Error(parametersNotUpToDateText);
        }

        runTest(toolCallParameters);
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

    const handleNewChat = () => {
        dispatch(editAiAgentActions.testDocumentSet(null));
        dispatch(editAiAgentActions.testMessagesSet([]));
        dispatch(editAiAgentActions.isWaitingForActionToolSubmitSet(false));
        testForm.setValue("prompt", "");
        testForm.setValue(
            "parameters",
            editFormValues.parameters.map((x) => ({
                name: x.name,
                value: testFormValues.parameters.find((y) => y.name === x.name)?.value ?? "",
            }))
        );
    };

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary hstack justify-content-between">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test agent
                    {(!hasLatestParameters || hasMissingParameters) && (
                        <PopoverWithHoverWrapper message={parametersNotUpToDateText}>
                            <Icon icon="warning" color="danger" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    )}
                </h3>
                <div className="hstack gap-2">
                    <Button
                        variant="primary"
                        size="sm"
                        onClick={handleNewChat}
                        className="rounded-pill"
                        title="Clear the current conversation and start a new chat"
                    >
                        New chat
                    </Button>
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
            </div>
            <div className="w-100 flex-grow-1 vstack justify-content-center align-items-center overflow-auto">
                <div className="flex-grow-1 vstack w-100 overflow-auto p-2 position-relative" ref={messagesPanelRef}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center">
                            <AiAgentParametersField
                                control={testForm.control}
                                name="parameters"
                                value={testFormValues.parameters}
                                isTest
                            />
                        </div>
                    )}
                    {!isRawData && messages.length > 0 && (
                        <AiAgentMessages
                            messages={messages}
                            toolQueries={configuration.Queries}
                            toolActions={configuration.Actions}
                            handleSaveParameters={handleSaveParameters}
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
                            control={testForm.control}
                            name="prompt"
                            placeholder="Ask the agent anything"
                            rows={3}
                            className="rounded-2"
                            disabled={isTestDisabled}
                            onKeyDown={(e) => {
                                if (e.key === "Enter" && !e.shiftKey) {
                                    e.preventDefault();
                                    testForm.handleSubmit(handleSend)();
                                }
                            }}
                        />
                        {testFormValues.prompt && (
                            <ButtonWithSpinner
                                variant="secondary"
                                icon="arrow-up"
                                onClick={testForm.handleSubmit(handleSend)}
                                isSpinning={isTestDisabled}
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

const parametersNotUpToDateText = "The parameters are not up to date. Please start a new chat.";
