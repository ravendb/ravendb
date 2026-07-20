import { useState } from "react";
import { useParams } from "react-router";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { PagePanel } from "@/components/data/page-panel";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { ConversationStatsCards, ConversationsSection } from "@/pages/apps/conversations-section";

export function AppConversations() {
    const { slug = "" } = useParams();
    const [period, setPeriod] = useState(getDefaultDatePeriod);

    return (
        <PagePanel>
            <div className="space-y-8">
                <div className="flex justify-end">
                    <DatePeriodPicker value={period} onChange={setPeriod} />
                </div>
                <ConversationStatsCards slug={slug} period={period} />
                <ConversationsSection slug={slug} period={period} />
            </div>
        </PagePanel>
    );
}
