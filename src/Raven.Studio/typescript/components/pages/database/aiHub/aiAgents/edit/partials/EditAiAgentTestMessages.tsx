import { useRef, useState } from "react";
import Button from "react-bootstrap/Button";
import { Modal } from "components/common/Modal";
import { Icon } from "components/common/Icon";
import AiAgentMessages from "../../partials/AiAgentMessages";
import { AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { useAppSelector } from "components/store";
import { editAiAgentSelectors } from "../store/editAiAgentSlice";
import { useAsyncCallback } from "react-async-hook";
import messagePublisher from "common/messagePublisher";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";
import { LazyLoad } from "components/common/LazyLoad";
import copyToClipboard from "common/copyToClipboard";

interface SubConversationData {
    document: documentDto;
    messages: AiAgentMessage[];
}

interface EditAiAgentTestMessagesProps {
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => Promise<void>;
}

export default function EditAiAgentTestMessages({ handleSaveParameters }: EditAiAgentTestMessagesProps) {
    const mainTestMessages = useAppSelector(editAiAgentSelectors.mainTestMessages);
    const mainTestDocument = useAppSelector(editAiAgentSelectors.mainTestDocument);
    const testDocuments = useAppSelector(editAiAgentSelectors.testDocuments);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();

    const [subConversationHistory, setSubConversationHistory] = useState<SubConversationData[]>([]);
    const currentSubConversationData = subConversationHistory[subConversationHistory.length - 1];

    const abortControllerRef = useRef<AbortController>(null);

    const asyncOpenSubConversation = useAsyncCallback(async (subConversationId: string) => {
        abortControllerRef.current?.abort();
        abortControllerRef.current = new AbortController();

        const conversationDocument = testDocuments[subConversationId];
        if (!conversationDocument) {
            messagePublisher.reportError(`Sub-conversation (${subConversationId}) data is not available`);
            return;
        }

        const agentId = conversationDocument.Agent;
        if (!agentId) {
            messagePublisher.reportError(`Agent ID is not available for sub-conversation (${subConversationId})`);
            return;
        }

        const agentResult = await aiAgentService.getAiAgents(databaseName, agentId, abortControllerRef.current.signal);
        const config = agentResult.AiAgents[0];
        if (!config) {
            messagePublisher.reportError(
                `Agent configuration is not available for sub-conversation (${subConversationId})`
            );
            return;
        }

        const subMessages = aiAgentsUtils.mapMessagesFromDoc({
            conversationDocument,
            config,
        });

        setSubConversationHistory((prev) => [
            ...prev,
            {
                messages: subMessages,
                document: conversationDocument,
            },
        ]);
    });

    const handleGoBack = () => {
        if (asyncOpenSubConversation.loading) {
            abortControllerRef.current?.abort();
            return;
        }

        setSubConversationHistory((prev) => prev.slice(0, prev.length - 1));
    };

    return (
        <>
            <AiAgentMessages
                mode="test"
                messages={mainTestMessages}
                handleSaveParameters={handleSaveParameters}
                parametersFromUser={mainTestDocument.Parameters}
                openActionCalls={mainTestDocument.OpenActionCalls}
                openTestSubConversation={asyncOpenSubConversation.execute}
            />
            {(currentSubConversationData || asyncOpenSubConversation.loading) && (
                <Modal show onHide={handleGoBack} size="xl">
                    <Modal.Header onCloseClick={handleGoBack}>
                        <div className="vstack gap-1 pe-4">
                            <h3>
                                <Icon icon="ai-agents" />
                                Sub-agent test transcript
                            </h3>
                            {currentSubConversationData?.document && (
                                <small className="text-muted text-break">
                                    Identifier: <b>{currentSubConversationData.document.Agent}</b>
                                    <Button
                                        variant="link"
                                        className="p-0 ms-1 align-baseline"
                                        onClick={() =>
                                            copyToClipboard.copy(
                                                currentSubConversationData.document.Agent,
                                                "Agent ID copied to clipboard"
                                            )
                                        }
                                        size="xs"
                                    >
                                        <Icon icon="copy" margin="m-0" />
                                    </Button>
                                </small>
                            )}
                            <small className="text-muted text-break">
                                This conversation exists only in the current test result and is not stored in the
                                database.
                            </small>
                        </div>
                    </Modal.Header>
                    {asyncOpenSubConversation.loading ? (
                        <Modal.Body className="pt-0">
                            <MessagesSkeleton />
                        </Modal.Body>
                    ) : (
                        <>
                            <Modal.Body className="overflow-auto" style={{ maxHeight: "65vh" }}>
                                <AiAgentMessages
                                    mode="test"
                                    messages={currentSubConversationData.messages}
                                    handleSaveParameters={handleSaveParameters}
                                    parametersFromUser={currentSubConversationData.document?.Parameters}
                                    openActionCalls={currentSubConversationData.document?.OpenActionCalls}
                                    openTestSubConversation={asyncOpenSubConversation.execute}
                                />
                            </Modal.Body>
                            <Modal.Footer>
                                <Button variant="secondary" onClick={handleGoBack} className="rounded-pill">
                                    <Icon icon="close" />
                                    Close
                                </Button>
                            </Modal.Footer>
                        </>
                    )}
                </Modal>
            )}
        </>
    );
}

function MessagesSkeleton() {
    return (
        <LazyLoad active className="vstack gap-3">
            {Array.from({ length: 4 }).map((_, index) => (
                <div style={{ height: 80 }} key={index} />
            ))}
        </LazyLoad>
    );
}
