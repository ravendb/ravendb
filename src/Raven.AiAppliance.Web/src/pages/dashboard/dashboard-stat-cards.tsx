import { useId } from "react";
import { Area, AreaChart, YAxis } from "recharts";
import { Card, CardContent } from "@/components/shadcn/ui/card";
import { ChartContainer, type ChartConfig } from "@/components/shadcn/ui/chart";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { formatCompact } from "@/lib/format";

export type DashboardStatCard = {
    label: string;
    value: number | undefined;
    isLoading: boolean;
    caption?: string;
    series?: number[];
};

export function DashboardStatCards({ cards }: { cards: DashboardStatCard[] }) {
    return (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {cards.map((card) => (
                <StatCard key={card.label} card={card} />
            ))}
        </div>
    );
}

function StatCard({ card }: { card: DashboardStatCard }) {
    const hasSparkline = card.series !== undefined && card.series.length > 1;

    return (
        <Card className="gap-3 pb-0">
            <CardContent className="space-y-1">
                <div className="text-sm text-muted-foreground">{card.label}</div>
                {card.isLoading ? (
                    <Skeleton className="h-9 w-20" />
                ) : (
                    <div className="text-3xl font-semibold tracking-tight tabular-nums">
                        {card.value === undefined ? "—" : formatCompact(card.value)}
                    </div>
                )}
                {card.caption && <div className="text-xs text-muted-foreground">{card.caption}</div>}
            </CardContent>
            {hasSparkline ? <Sparkline series={card.series!} /> : <div className="h-14" aria-hidden="true" />}
        </Card>
    );
}

const sparklineConfig = {
    value: { label: "Value", color: "var(--chart-1)" },
} satisfies ChartConfig;

function Sparkline({ series }: { series: number[] }) {
    const gradientId = `stat-sparkline-${useId().replace(/:/g, "")}`;
    const data = series.map((value, index) => ({ index, value }));

    return (
        <ChartContainer config={sparklineConfig} className="aspect-auto h-14 w-full">
            <AreaChart data={data} margin={{ top: 6, right: 0, bottom: 0, left: 0 }}>
                <defs>
                    <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="var(--color-value)" stopOpacity={0.35} />
                        <stop offset="100%" stopColor="var(--color-value)" stopOpacity={0} />
                    </linearGradient>
                </defs>
                <YAxis hide domain={[0, "dataMax"]} />
                <Area
                    dataKey="value"
                    type="monotone"
                    stroke="var(--color-value)"
                    strokeWidth={2}
                    fill={`url(#${gradientId})`}
                    dot={false}
                    isAnimationActive={false}
                />
            </AreaChart>
        </ChartContainer>
    );
}
