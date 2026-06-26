import { useParams } from "react-router";
import { PagePanel } from "@/components/data/page-panel";
import { ConversationStatsCards, ConversationsSection } from "@/pages/apps/conversations-section";

export function AppConversations() {
    const { slug = "" } = useParams();

    return (
        <PagePanel>
            <div className="space-y-8">
                <ConversationStatsCards slug={slug} />
                <ConversationsSection slug={slug} />
            </div>
        </PagePanel>
    );
}
