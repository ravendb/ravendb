import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { getDefaultDatePeriod } from "@/lib/date-period";
import { useAppStartDate } from "@/lib/use-start-date";
import { StatCardsSection } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";

export function StatisticsSection({ slug }: { slug: string }) {
    const [period, setPeriod] = useState(getDefaultDatePeriod);
    const appStartDate = useAppStartDate(slug);
    const usageQuery = useQuery(api.queries.stats.usage(period, slug));

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);

    // The period governs nothing but these tiles on the overview, so the picker rides in the
    // section header rather than at page level.
    return (
        <StatCardsSection
            cards={cards}
            action={<DatePeriodPicker value={period} earliest={appStartDate} onChange={setPeriod} />}
        />
    );
}
