import { useId } from "react";
import { TrendingDown, TrendingUp } from "lucide-react";
import { Area, AreaChart, YAxis } from "recharts";
import { Badge } from "@/components/shadcn/ui/badge";
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
    // Preformatted value, used when formatCompact isn't enough (e.g. currency).
    valueLabel?: string;
    // Period-over-period change as a percent (12.5 -> +12.5%). Renders a trend badge.
    delta?: number;
};

export function DashboardStatCards({ cards }: { cards: DashboardStatCard[] }) {
    return (
        // Two-up on small screens; a single equal-width row from lg up, for any card count.
        <div className="grid grid-cols-2 gap-4 lg:auto-cols-fr lg:grid-flow-col lg:grid-cols-none">
            {cards.map((card) => (
                <StatCard key={card.label} card={card} />
            ))}
        </div>
    );
}

function StatCard({ card }: { card: DashboardStatCard }) {
    const valueLabel = card.valueLabel ?? (card.value === undefined ? "—" : formatCompact(card.value));

    return (
        <Card className="gap-3">
            <CardContent className="space-y-1">
                <div className="flex items-center justify-between gap-2">
                    <span className="text-sm text-muted-foreground">{card.label}</span>
                    {card.delta !== undefined && !card.isLoading && <DeltaBadge delta={card.delta} />}
                </div>
                {card.isLoading ? (
                    <Skeleton className="h-9 w-20" />
                ) : (
                    <div className="text-3xl font-semibold tracking-tight tabular-nums">{valueLabel}</div>
                )}
                {card.caption && <div className="text-xs text-muted-foreground">{card.caption}</div>}
            </CardContent>
            {card.series && card.series.length > 1 && <Sparkline series={card.series} />}
        </Card>
    );
}

function DeltaBadge({ delta }: { delta: number }) {
    const isUp = delta >= 0;
    const Icon = isUp ? TrendingUp : TrendingDown;

    return (
        <Badge variant={isUp ? "success" : "destructive"}>
            <Icon aria-hidden="true" />
            {isUp ? "+" : ""}
            {delta.toFixed(1)}%
        </Badge>
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
