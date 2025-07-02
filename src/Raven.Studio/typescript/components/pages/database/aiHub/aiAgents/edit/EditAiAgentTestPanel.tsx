import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "./utils/editAiAgentValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { editAiAgentActions, EditAiAgentMessage, editAiAgentSelectors } from "./store/editAiAgentSlice";
import { FormInput } from "components/common/Form";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import Badge from "react-bootstrap/Badge";
import genUtils from "common/generalUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import AceEditor from "components/common/ace/AceEditor";
import moment from "moment";
import ReactAce from "react-ace";
import { useRef, useEffect } from "react";
import _ from "lodash";
import Spinner from "react-bootstrap/Spinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

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
        if (!formValues.testPrompt) {
            return;
        }

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
        } catch (error) {
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
                        {messages.length === 0 ? <ParametersField /> : <Messages />}
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

function Messages() {
    const messages = useAppSelector(editAiAgentSelectors.messages);

    return (
        <div className="w-100 vstack gap-2">
            {messages.map((x, idx) =>
                x.author === "user" ? (
                    <div key={idx} className="hstack justify-content-end">
                        <div
                            className="text-end bg-faded-primary p-2 rounded-3 border border-primary text-reset"
                            style={{ maxWidth: "75%" }}
                        >
                            {x.text}
                        </div>
                    </div>
                ) : (
                    <AgentMessage key={idx} agentMessage={x} />
                )
            )}
        </div>
    );
}

function AgentMessage({ agentMessage }: { agentMessage: EditAiAgentMessage }) {
    const aceRef = useRef<ReactAce>(null);

    return (
        <div>
            <div className="hstack justify-content-between mb-2">
                <div className="hstack gap-2">
                    <div className="p-1 rounded-2 border">
                        <Icon icon="sparkles" margin="m-0" />
                    </div>
                    <strong>AI Agent</strong>
                    <div className="text-muted">{moment(agentMessage.date).format("HH:mm A")}</div>
                </div>
                {agentMessage.usage && (
                    <div className="hstack text-muted">
                        <PopoverWithHoverWrapper
                            message={
                                <div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Prompt tokens</span>
                                        <span>{agentMessage.usage.PromptTokens}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Completion tokens</span>
                                        <span>{agentMessage.usage.CompletionTokens}</span>
                                    </div>
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Cached tokens</span>
                                        <span>{agentMessage.usage.CachedTokens}</span>
                                    </div>
                                    <hr className="my-1" />
                                    <div className="hstack justify-content-between gap-3">
                                        <span>Tokens usage</span>
                                        <span>{agentMessage.usage.TotalTokens}</span>
                                    </div>
                                </div>
                            }
                        >
                            <Icon icon="info" />
                        </PopoverWithHoverWrapper>
                        Tokens usage: {agentMessage.usage.TotalTokens}
                    </div>
                )}
            </div>
            {agentMessage.state === "loading" && (
                <div className="hstack">
                    <Spinner size="sm" className="me-1" />
                    <span>Thinking...</span>
                </div>
            )}
            {agentMessage.state === "error" && <div className="text-danger">Error</div>}
            {agentMessage.state === "success" && (
                <AceEditor
                    aceRef={aceRef}
                    value={agentMessage.text}
                    readOnly
                    mode="json"
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                    height={getAgentAceEditorHeight(agentMessage.text)}
                />
            )}
        </div>
    );
}

function getAgentAceEditorHeight(text: string): `${number}px` {
    if (!text) {
        return "100px";
    }

    const lineHeight = 26;
    const lineCount = text.split("\n").length;

    if (lineCount <= 12) {
        return `${lineCount * lineHeight}px`;
    }

    return "320px";
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
        Persistence: {
            Collection: formValues.persistenceCollectionName,
            Expires: genUtils.formatAsTimeSpan(formValues.persistenceExpiresInSeconds * 1000),
        },
        Queries: formValues.queries.map((x) => ({
            Name: x.name,
            Description: x.description,
            Query: x.query,
            ParametersSchema: x.parametersSchema,
        })),
        Actions: formValues.actions.map((x) => ({
            Name: x.name,
            Description: x.description,
            ParametersSchema: x.parametersSchema,
        })),
        Prompt: formValues.testPrompt,
        Parameters: Object.fromEntries(formValues.testParameters.map((item) => [item.name, item.value])),
    };
}
