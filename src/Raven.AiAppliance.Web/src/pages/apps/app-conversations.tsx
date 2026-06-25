import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ChatConsole } from "@/components/app/chat-console";
import { PagePanel } from "@/components/data/page-panel";
import { RawDataPreview } from "@/components/data/raw-data-preview";

export function AppConversations() {
    const { slug = "" } = useParams();
    const conversationsQuery = useQuery(api.queries.stats.conversations(slug));
    const conversationStatsQuery = useQuery(api.queries.stats.conversationStats(slug));

    // Pull the first conversation's full record so the single-conversation endpoint
    // (transcript included) is visible too.
    const firstConversationId = conversationsQuery.data?.[0]?.id;
    const conversationQuery = useQuery({
        ...api.queries.stats.conversation(slug, firstConversationId ?? ""),
        enabled: Boolean(firstConversationId),
    });

    return (
        <PagePanel>
            <div className="space-y-6">
                <RawDataPreview title="stats.conversations" query={conversationsQuery} />
                <RawDataPreview title="stats.conversationStats" query={conversationStatsQuery} />
                <RawDataPreview title="stats.conversation (first)" query={conversationQuery} />
                <ChatConsole key={slug} defaultAgentId={slug} />
            </div>
        </PagePanel>
    );
}
