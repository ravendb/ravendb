import { useParams } from "react-router";
import { ChatConsole } from "@/components/app/chat-console";
import { PagePanel } from "@/components/data/page-panel";

export function AppConversations() {
    const { appId } = useParams();

    return (
        <PagePanel>
            <ChatConsole key={appId} defaultAgentId={appId} />
        </PagePanel>
    );
}
