import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { editAiAgentActions, editAiAgentSelectors } from "./store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import genUtils from "common/generalUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useRef, useEffect } from "react";
import _ from "lodash";
import AiAgentMessages from "../partials/AiAgentMessages";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import moment from "moment";
import { editAiAgentUtils } from "./utils/editAiAgentUtils";
import RichAlert from "components/common/RichAlert";

export default function EditAiAgentTestPanel() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);
    const messages = useAppSelector(editAiAgentSelectors.messages);

    const { aiAgentService } = useServices();

    const asyncHandleTest = useAsyncCallback(async () => {
        dispatch(
            editAiAgentActions.messagesAdd({
                id: _.uniqueId(),
                content: formValues.testPrompt,
                role: "user",
                state: "success",
                date: moment().format("HH:mm A"),
            })
        );

        const agentMessageId = _.uniqueId();

        dispatch(
            editAiAgentActions.messagesAdd({
                id: agentMessageId,
                role: "assistant",
                date: moment().format("HH:mm A"),
                state: "loading",
            })
        );

        try {
            const result = await aiAgentService.testAiAgent(
                databaseName,
                getConfigDto(formValues),
                formValues.testPrompt,
                Object.fromEntries(formValues.testParameters.map((item) => [item.name, item.value]))
            );

            setValue("testPrompt", "");
            dispatch(
                editAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    content: JSON.stringify(result.Response, null, 2),
                    state: "success",
                    usage: result.Usage,
                    toolCalls: Object.entries(result.ToolRequests).map(([_, toolCall]) => ({
                        id: toolCall.ToolId,
                        name: toolCall.Name,
                        arguments: toolCall.Arguments,
                    })),
                })
            );
        } catch {
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
                <RichAlert variant="danger">
                    TODO
                    <br />
                    For now test can only start chat. Resume will be added later.
                </RichAlert>
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
                                handleSaveParameters={(parameters) =>
                                    dispatch(editAiAgentActions.toolParametersSet(parameters))
                                }
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
                                    onClick={asyncHandleTest.execute}
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

function getConfigDto(
    formValues: EditAiAgentFormData
): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration {
    return {
        ConnectionStringName: formValues.connectionStringName,
        SystemPrompt: formValues.systemPrompt,
        OutputSchema: formValues.outputSchema,
        SampleObject: formValues.sampleObject,
        Persistence: {
            Collection: formValues.persistenceCollectionName,
            Expires: genUtils.formatAsTimeSpan(formValues.persistenceExpiresInSeconds * 1000),
        },
        Queries: formValues.queries.map((x) => ({
            Name: x.name,
            Description: x.description,
            Query: x.query,
            ParametersSampleObject: x.parametersSampleObject,
            ParametersSchema: x.parametersSchema,
        })),
        Actions: formValues.actions.map((x) => ({
            Name: x.name,
            Description: x.description,
            ParametersSampleObject: x.parametersSampleObject,
            ParametersSchema: x.parametersSchema,
        })),
        Parameters: formValues.parameters.map((x) => x.name),
    };
}
