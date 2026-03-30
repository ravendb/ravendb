import { useState } from "react";
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

interface SubConversationData {
    conversationId: string;
    messages: AiAgentMessage[];
    document: documentDto;
}

interface EditAiAgentTestMessagesProps {
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => void;
}

export default function EditAiAgentTestMessages({ handleSaveParameters }: EditAiAgentTestMessagesProps) {
    const mainTestMessages = useAppSelector(editAiAgentSelectors.mainTestMessages);
    const mainTestDocument = useAppSelector(editAiAgentSelectors.mainTestDocument);
    const testDocuments = useAppSelector(editAiAgentSelectors.testDocuments);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();

    const [subConversationHistory, setSubConversationHistory] = useState<SubConversationData[]>([]);
    const currentSubConversationData = subConversationHistory[subConversationHistory.length - 1];

    const asyncOpenSubConversation = useAsyncCallback(async (subConversationId: string) => {
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

        const agentResult = await aiAgentService.getAiAgents(databaseName, agentId);
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
                conversationId: subConversationId,
                messages: subMessages,
                document: conversationDocument,
            },
        ]);
    });

    const handleGoBack = () => {
        if (asyncOpenSubConversation.loading) {
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
                            <Modal.Body className="pt-0">
                                <div className="vstack gap-3">
                                    <div className="panel-bg-2 border border-secondary rounded-2 p-2 hstack justify-content-between align-items-start gap-2">
                                        <div className="overflow-hidden">
                                            <div className="fw-semibold text-truncate">Conversation ID</div>
                                            <small className="text-muted text-break">
                                                {currentSubConversationData.conversationId}
                                            </small>
                                        </div>
                                    </div>
                                    <div className="overflow-auto" style={{ maxHeight: "65vh" }}>
                                        <AiAgentMessages
                                            mode="test"
                                            messages={currentSubConversationData.messages}
                                            handleSaveParameters={handleSaveParameters}
                                            parametersFromUser={currentSubConversationData.document?.Parameters}
                                            openActionCalls={currentSubConversationData.document?.OpenActionCalls}
                                            openTestSubConversation={asyncOpenSubConversation.execute}
                                        />
                                    </div>
                                </div>
                            </Modal.Body>
                            <Modal.Footer>
                                <Button variant="secondary" onClick={handleGoBack}>
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
