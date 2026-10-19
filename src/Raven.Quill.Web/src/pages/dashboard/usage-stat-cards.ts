import type { UsagePoint, UsageResponse } from "@/api/generated/server-api";
import { WRU_DESCRIPTION, WRU_UNAVAILABLE_CAPTION } from "@/components/data/wru-label";
import type { DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";

// The "My apps" / app-overview stat cards, derived from the usage series: each card totals
// one metric across the window and sparklines its per-bucket shape.
export function buildUsageStatCards(usage: UsageResponse | undefined, isPending: boolean): DashboardStatCard[] {
    const points = usage?.points;
    const toCard = (label: string, select: (point: UsagePoint) => number): DashboardStatCard => ({
        label,
        value: points?.reduce((sum, point) => sum + select(point), 0),
        isLoading: isPending,
        series: points?.map(select),
        seriesDates: points?.map((point) => point.timestamp),
    });

    const wruCard: DashboardStatCard = usage?.isWritesUnavailable
        ? {
              label: "WRU",
              labelInfo: WRU_DESCRIPTION,
              value: undefined,
              isLoading: false,
              caption: WRU_UNAVAILABLE_CAPTION,
          }
        : { ...toCard("WRU", (point) => point.writes), labelInfo: WRU_DESCRIPTION };

    return [
        toCard("Conversations", (point) => point.conversations),
        toCard("Prompts", (point) => point.messages),
        toCard("Tokens", (point) => point.tokens),
        wruCard,
    ];
}
