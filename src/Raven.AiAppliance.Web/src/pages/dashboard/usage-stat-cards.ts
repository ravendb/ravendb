import type { UsagePoint } from "@/api/generated/server-api";
import type { DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";

// The "My apps" / app-overview stat cards, derived from the usage series: each card totals
// one metric across the window and sparklines its per-bucket shape.
export function buildUsageStatCards(points: UsagePoint[] | undefined, isPending: boolean): DashboardStatCard[] {
    const toCard = (label: string, select: (point: UsagePoint) => number): DashboardStatCard => ({
        label,
        value: points?.reduce((sum, point) => sum + select(point), 0),
        isLoading: isPending,
        series: points?.map(select),
    });

    return [
        toCard("Conversations", (point) => point.conversations),
        toCard("Messages", (point) => point.messages),
        toCard("Tokens", (point) => point.tokens),
        toCard("Writes", (point) => point.writes),
    ];
}
