import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { DashboardStatCards } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

export function StatisticsSection({ slug }: { slug: string }) {
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const usageQuery = useQuery(api.queries.stats.usage(period, slug));

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);

    return (
        <SectionCard title="Statistics" action={<DatePeriodPicker value={period} onChange={setPeriod} />}>
            <DashboardStatCards cards={cards} />
        </SectionCard>
    );
}
