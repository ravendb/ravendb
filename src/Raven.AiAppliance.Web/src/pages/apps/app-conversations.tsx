import { useParams } from "react-router";
import { ChatConsole } from "@/components/app/chat-console";
import { PagePanel } from "@/components/data/page-panel";

export function AppConversations() {
    const { slug = "" } = useParams();

    return (
        <PagePanel>
            <ChatConsole key={slug} defaultAgentId={slug} />
        </PagePanel>
    );
}
