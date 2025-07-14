import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { editAiAgentActions, editAiAgentSelectors } from "./store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useRef, useEffect } from "react";
import _ from "lodash";
import AiAgentMessages from "../partials/AiAgentMessages";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import moment from "moment";
import { editAiAgentUtils } from "./utils/editAiAgentUtils";
import { aiAgentsUtils } from "../utils/aiAgentsUtils";
import { AiAgentToolCall } from "../utils/aiAgentsTypes";

export default function EditAiAgentTestPanel() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);
    const messages = useAppSelector(editAiAgentSelectors.testMessages);
    const testDocument = useAppSelector(editAiAgentSelectors.testDocument);

    const { aiAgentService } = useServices();

    const asyncHandleTest = useAsyncCallback(async (toolParameters?: AiAgentToolCall[]) => {
        dispatch(
            editAiAgentActions.testMessagesAdd({
                id: _.uniqueId(),
                content: formValues.testPrompt,
                role: "user",
                state: "success",
                date: moment().format(aiAgentsUtils.messageDateFormat),
                toolCalls: toolParameters,
            })
        );

        const agentMessageId = _.uniqueId();

        dispatch(
            editAiAgentActions.testMessagesAdd({
                id: agentMessageId,
                role: "assistant",
                date: moment().format(aiAgentsUtils.messageDateFormat),
                state: "loading",
            })
        );

        try {
            const result = await aiAgentService.testAiAgent(databaseName, {
                Configuration: editAiAgentUtils.mapToDto(formValues),
                UserPrompt: toolParameters?.length > 0 ? null : formValues.testPrompt,
                Parameters: Object.fromEntries(formValues.testParameters.map((item) => [item.name, item.value])),
                ActionResponses: toolParameters?.map((x) => ({
                    ToolId: x.id,
                    Content: x.arguments,
                })),
                Document: testDocument,
                RequestBody: undefined,
            });

            dispatch(editAiAgentActions.testDocumentSet(result.Document));
            setValue("testPrompt", "");
            dispatch(
                editAiAgentActions.messagesUpdate(
                    aiAgentsUtils.mapMessageFromResponse(result, agentMessageId, result.Document)
                )
            );
        } catch (e) {
            console.error(e);
            dispatch(
                editAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    state: "error",
                })
            );
        }
    });

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

    return (
        <>
            <div className="panel-bg-2 p-3 border-bottom border-secondary">
                <h3 className="m-0">
                    <Icon icon="test" color="primary" />
                    Test results
                </h3>
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
                    <div className="flex-grow-1 vstack w-100 overflow-auto p-2" ref={messagesPanelRef}>
                        {messages.length === 0 ? (
                            <AiAgentParametersField
                                control={control}
                                name="testParameters"
                                value={formValues.testParameters}
                            />
                        ) : (
                            <AiAgentMessages
                                messages={messages}
                                toolQueries={Queries}
                                toolActions={Actions}
                                handleSaveParameters={(parameters) => asyncHandleTest.execute(parameters)}
                            />
                        )}
                    </div>
                    <div className="w-100 p-2 panel-bg-2 border-top border-secondary">
                        <div className="position-relative">
                            <FormInput
                                type="textarea"
                                as="textarea"
                                control={control}
                                name="testPrompt"
                                placeholder="Message an agent"
                                rows={3}
                                className="rounded-2"
                                style={{ resize: "none" }}
                                disabled={asyncHandleTest.loading}
                            />
                            {formValues.testPrompt && (
                                <ButtonWithSpinner
                                    variant="secondary"
                                    icon="arrow-up"
                                    onClick={() => asyncHandleTest.execute()}
                                    isSpinning={asyncHandleTest.loading}
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
