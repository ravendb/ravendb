import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { USAGE_WINDOW_BY_KEY } from "@/components/data/usage-window";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { DashboardStatCards } from "@/pages/dashboard/dashboard-stat-cards";
import { buildUsageStatCards } from "@/pages/dashboard/usage-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

export function StatisticsSection({ slug }: { slug: string }) {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");
    const usageQuery = useQuery(api.queries.stats.usage(USAGE_WINDOW_BY_KEY[windowKey], slug));

    const cards = buildUsageStatCards(usageQuery.data, usageQuery.isPending);

    return (
        <SectionCard title="Statistics" action={<WindowTabs value={windowKey} onChange={setWindowKey} />}>
            <DashboardStatCards cards={cards} />
        </SectionCard>
    );
}
