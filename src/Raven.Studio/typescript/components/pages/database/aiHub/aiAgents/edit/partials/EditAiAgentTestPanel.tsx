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
import AiAgentParametersField from "../../partials/AiAgentParametersField";
import { editAiAgentUtils } from "../utils/editAiAgentUtils";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import SizeGetter from "components/common/SizeGetter";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import { tryHandleSubmit } from "components/utils/common";
import messagePublisher from "common/messagePublisher";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { compareSets } from "common/typeUtils";
import AiAgentParametersDropdown from "../../partials/AiAgentParametersDropdown";
import classNames from "classnames";
import { AiAgentToolCall } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";
import EditAiAgentTestMessages from "./EditAiAgentTestMessages";

interface EditAiAgentTestPanelProps {
    testForm: UseFormReturn<TestAiAgentFormData>;
    editForm: UseFormReturn<EditAiAgentFormData>;
    generateTestParameters: () => void;
}

export default function EditAiAgentTestPanel({
    testForm,
    editForm,
    generateTestParameters,
}: EditAiAgentTestPanelProps) {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const rawDataRef = useRef<ReactAce>(null);

    const testFormValues = useWatch({
        control: testForm.control,
    });
    const editFormValues = useWatch({
        control: editForm.control,
    });

    const testDocument = useAppSelector(editAiAgentSelectors.mainTestDocument);
    const messages = useAppSelector(editAiAgentSelectors.mainTestMessages);
    const isRawData = useAppSelector(editAiAgentSelectors.isRawData);
    const runTestState = useAppSelector(editAiAgentSelectors.runTestState);
    const isActionToolSubmitRequired = useAppSelector(editAiAgentSelectors.isActionToolSubmitRequired);
    const isTestPinned = useAppSelector(editAiAgentSelectors.isTestPinned);

    const hasLatestParameters = compareSets(
        editFormValues.parameters?.map((x) => x.name) ?? [],
        testFormValues.parameters?.map((x) => x.name) ?? []
    );

    const hasMissingParameters =
        messages.length > 0 && testFormValues.parameters.some((x) => x.type !== "Null" && x.value == null);

    const isLoading = runTestState === "loading";
    const isTestDisabled = !hasLatestParameters || hasMissingParameters || isLoading || isActionToolSubmitRequired;

    const configuration = editAiAgentUtils.mapToDto(editFormValues);

    const runTest = async (toolCallParameters?: AiAgentToolCall[]) => {
        return tryHandleSubmit(async () => {
            await dispatch(
                editAiAgentActions.runTest({
                    databaseName,
                    configuration,
                    testFormValues,
                    toolCallParameters,
                })
            ).unwrap();

            testForm.setValue("prompt", "");
        });
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            await runTest();
        });
    };

    const handleSaveParameters = async (toolCallParameters?: AiAgentToolCall[]) => {
        if (!hasLatestParameters) {
            messagePublisher.reportError(parametersNotUpToDateText);
            throw new Error(parametersNotUpToDateText);
        }

        await runTest(toolCallParameters);
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
        dispatch(editAiAgentActions.testResultsReset());
        testForm.setValue("prompt", "");
        generateTestParameters();
    };

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary d-flex flex-wrap justify-content-between gap-2">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test agent
                    {(!hasLatestParameters || hasMissingParameters) && (
                        <PopoverWithHoverWrapper message={parametersNotUpToDateText}>
                            <Icon icon="warning" color="danger" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    )}
                </h3>
                <div className="d-flex gap-2 flex-wrap flex-grow justify-content-end">
                    <Button
                        variant="primary"
                        onClick={handleNewChat}
                        className="rounded-pill"
                        title="Clear the current conversation and start a new chat"
                    >
                        <Icon icon="plus" />
                        New chat
                    </Button>
                    {testDocument?.Parameters && (
                        <AiAgentParametersDropdown parameters={testDocument.Parameters} isCompact />
                    )}
                    {testDocument && messages.length > 0 && (
                        <Button
                            variant="secondary"
                            onClick={() => dispatch(editAiAgentActions.isRawDataSet(!isRawData))}
                            className="rounded-pill"
                            title="Switch between chat and raw data display"
                        >
                            <Icon icon={isRawData ? "ai-agents" : "json"} margin="m-0" />
                        </Button>
                    )}
                    <div className="d-flex align-items-center">
                        <Button
                            variant="link"
                            onClick={() => dispatch(editAiAgentActions.isTestPinnedSet(!isTestPinned))}
                            className={classNames({ "text-reset": !isTestPinned })}
                        >
                            <Icon icon={isTestPinned ? "pinned" : "pin"} margin="m-0" />
                        </Button>
                        <Button
                            variant="link"
                            onClick={() => dispatch(editAiAgentActions.isTestOpenSet(false))}
                            className="text-reset"
                        >
                            <Icon icon="close" margin="m-0" />
                        </Button>
                    </div>
                </div>
            </div>
            <div className="w-100 flex-grow-1 vstack justify-content-center align-items-center overflow-auto">
                <div className="flex-grow-1 vstack w-100 overflow-auto p-2 position-relative" ref={messagesPanelRef}>
                    {messages.length === 0 && (
                        <div className="h-100 vstack justify-content-center mx-auto" style={{ maxWidth: "600px" }}>
                            <AiAgentParametersField
                                control={testForm.control}
                                value={testFormValues.parameters}
                                panelClassName="panel-bg-2"
                                wrapperClassName="justify-content-center"
                                headerClassName="text-center"
                            />
                        </div>
                    )}
                    {!isRawData && messages.length > 0 && (
                        <EditAiAgentTestMessages handleSaveParameters={handleSaveParameters} />
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
                                icon="arrow-thin-top"
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
