import { PropsWithChildren, createContext, useContext } from "react";
import { AiAgentOpenActionCalls, AiAgentToolCall } from "components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes";

export interface AiAgentMessagesCommonContextValue {
    handleSaveParameters: (toolCallParameters: AiAgentToolCall[]) => Promise<void>;
    parametersFromUser?: Record<string, string>;
    documentId?: string;
    openActionCalls?: AiAgentOpenActionCalls;
}

export type AiAgentMessagesContextValue = AiAgentMessagesCommonContextValue &
    (
        | {
              mode: "chat";
          }
        | {
              mode: "test";
              openTestSubConversation: (subConversationId: string) => void;
          }
    );

const AiAgentMessagesContext = createContext<AiAgentMessagesContextValue | null>(null);

interface AiAgentMessagesProviderProps extends PropsWithChildren {
    value: AiAgentMessagesContextValue;
}

export function AiAgentMessagesProvider({ value, children }: AiAgentMessagesProviderProps) {
    return <AiAgentMessagesContext.Provider value={value}>{children}</AiAgentMessagesContext.Provider>;
}

export function useAiAgentMessagesContext() {
    const context = useContext(AiAgentMessagesContext);

    if (!context) {
        throw new Error("AiAgentMessagesContext is not available");
    }

    return context;
}
