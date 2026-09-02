import { useState } from "react";
import { useParams } from "react-router";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { useAppStartDate } from "@/lib/use-start-date";
import { ConversationStatsCards, ConversationsSection } from "@/pages/apps/conversations-section";
import { Heading, Text } from "@/components/typography";

export function AppConversations() {
    const { slug = "" } = useParams();
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const appStartDate = useAppStartDate(slug);

    return (
        <div className="space-y-8">
            <div className="space-y-6">
                <div className="flex items-start justify-between gap-3">
                    <div className="space-y-1">
                        <Heading as="h1" variant="page">
                            Conversations
                        </Heading>
                        <Text variant="muted">Live and historical chats across all channels.</Text>
                    </div>
                    <DatePeriodPicker value={period} earliest={appStartDate} onChange={setPeriod} />
                </div>
                <ConversationStatsCards slug={slug} period={period} />
            </div>
            <ConversationsSection slug={slug} period={period} />
        </div>
    );
}
