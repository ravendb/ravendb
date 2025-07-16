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

export default function EditAiAgentTestPanel() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);
    const messages = useAppSelector(editAiAgentSelectors.testMessages);
    const runTestState = useAppSelector(editAiAgentSelectors.runTestState);

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

    const handleReset = () => {
        dispatch(editAiAgentActions.isTestOpenSet(false));
        dispatch(editAiAgentActions.testDocumentSet(null));
        dispatch(editAiAgentActions.testMessagesSet([]));
        setValue("test.prompt", "");
    };

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary hstack justify-content-between">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test results
                </h3>
                <Button variant="secondary" size="sm" onClick={handleReset} className="rounded-pill">
                    <Icon icon="reset" />
                    Reset
                </Button>
            </div>
            {!isTestOpen && (
                <div className="p-3 flex-grow-1 vstack justify-content-center align-items-center">
                    <Icon icon="test" color="primary" className="fs-1" />
                    <p className="mt-2 text-center">
                        This is a testing environment for your AI Agent.
                        <br />
                        Once everything is configured, click the &quot;Test&quot; button to see the results.
                    </p>
                </div>
            )}
            {isTestOpen && (
                <div className="w-100 flex-grow-1 vstack justify-content-center align-items-center overflow-auto">
                    <div
                        className="flex-grow-1 vstack w-100 overflow-auto p-2 position-relative"
                        ref={messagesPanelRef}
                    >
                        {messages.length === 0 && (
                            <AiAgentParametersField
                                control={control}
                                name="test.parameters"
                                value={formValues.test.parameters}
                            />
                        )}
                        {messages.length > 0 && (
                            <AiAgentMessages
                                messages={messages}
                                toolQueries={Queries}
                                toolActions={Actions}
                                handleSaveParameters={(toolCallParameters) => runTest(toolCallParameters)}
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
                                placeholder="Message an agent"
                                rows={3}
                                className="rounded-2"
                                style={{ resize: "none" }}
                                disabled={runTestState === "loading"}
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
                                    isSpinning={runTestState === "loading"}
                                    className="position-absolute rounded-pill"
                                    style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                                />
                            )}
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}
