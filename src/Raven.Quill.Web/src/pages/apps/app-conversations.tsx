import { useState } from "react";
import { useParams } from "react-router";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { useAppStartDate } from "@/lib/use-start-date";
import { ConversationStatsCards, ConversationsSection } from "@/pages/apps/conversations-section";

export function AppConversations() {
    const { slug = "" } = useParams();
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const appStartDate = useAppStartDate(slug);

    return (
        <div className="space-y-8">
            <div className="space-y-6">
                <div className="space-y-1">
                    <h1 className="text-2xl font-semibold tracking-tight">Conversations</h1>
                    <p className="text-sm text-muted-foreground">Live and historical chats across all channels.</p>
                </div>
                <ConversationStatsCards
                    slug={slug}
                    period={period}
                    earliest={appStartDate}
                    onPeriodChange={setPeriod}
                />
            </div>
            <ConversationsSection slug={slug} period={period} />
        </div>
    );
}
