import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { editAiAgentActions, editAiAgentSelectors } from "./store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import Badge from "react-bootstrap/Badge";
import genUtils from "common/generalUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useRef, useEffect } from "react";
import _ from "lodash";
import AiAgentMessages from "../partials/AiAgentMessages";

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
                text: formValues.testPrompt,
                author: "user",
                state: "success",
            })
        );

        const agentMessageId = _.uniqueId();

        dispatch(
            editAiAgentActions.messagesAdd({
                id: agentMessageId,
                author: "agent",
                date: new Date(),
                state: "loading",
            })
        );

        try {
            const result = await aiAgentService.testAiAgent(databaseName, getTestDto(formValues));
            setValue("testPrompt", "");
            dispatch(
                editAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    text: JSON.stringify(result.Response, null, 2),
                    state: "success",
                    usage: result.Usage,
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

    const promptNewLinesCount = formValues.testPrompt?.split("\n").length ?? 1;

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
                        {messages.length === 0 ? <ParametersField /> : <AiAgentMessages messages={messages} />}
                    </div>
                    <div className="w-100 p-2 panel-bg-2 border-top border-secondary">
                        <FormInput
                            type="textarea"
                            as="textarea"
                            control={control}
                            name="testPrompt"
                            placeholder="Message an agent"
                            rows={promptNewLinesCount}
                            disabled={asyncHandleTest.loading}
                        />
                        <div className="hstack justify-content-end mt-2">
                            <ButtonWithSpinner
                                variant="primary"
                                icon="arrow-up"
                                onClick={asyncHandleTest.execute}
                                disabled={!formValues.testPrompt}
                                isSpinning={asyncHandleTest.loading}
                            />
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

function ParametersField() {
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    if (formValues.testParameters.length === 0) {
        return null;
    }

    return (
        <div className="flex-grow-1 text-center w-100">
            <Icon icon="metrics" color="primary" size="lg" className="mt-3" />
            <h4>To ask your agent a question select a chosen parameters.</h4>

            {formValues.testParameters.map((x, idx) => (
                <div className="w-100 p-2">
                    <div key={x.name} className="hstack">
                        <Badge
                            bg="primary"
                            className="text-truncate me-2 fs-5"
                            title={x.name}
                            style={{ width: "150px" }}
                            pill
                        >
                            {x.name}
                        </Badge>
                        <FormInput
                            type="text"
                            control={control}
                            name={`testParameters.${idx}.value`}
                            placeholder="e.g. companies/90-A"
                        />
                    </div>
                    {idx !== formValues.testParameters.length - 1 && <hr className="my-1" />}
                </div>
            ))}
        </div>
    );
}

function getTestDto(
    formValues: EditAiAgentFormData
): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration & {
    Parameters: Record<string, string>;
    Prompt: string;
} {
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
        Prompt: formValues.testPrompt,
        Parameters: Object.fromEntries(formValues.testParameters.map((item) => [item.name, item.value])),
    };
}
