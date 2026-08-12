import { useId, type ReactNode } from "react";
import { TrendingDown, TrendingUp } from "lucide-react";
import { Area, AreaChart, YAxis } from "recharts";
import { ZERO_SAFE_Y_DOMAIN } from "@/lib/chart-domain";
import { InfoHint } from "@/components/data/info-hint";
import { Badge } from "@/components/shadcn/ui/badge";
import { Card, CardContent } from "@/components/shadcn/ui/card";
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/shadcn/ui/chart";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { formatCompact } from "@/lib/format";
import { SectionCard } from "@/pages/apps/section-card";

export type DashboardStatCard = {
    label: string;
    // Tooltip shown on an info icon next to the label (e.g. expanding an abbreviation).
    labelInfo?: string;
    value: number | undefined;
    isLoading: boolean;
    caption?: string;
    series?: number[];
    // ISO timestamps aligned 1:1 with `series`; when present, the sparkline tooltip shows the date.
    seriesDates?: string[];
    // Preformatted value, used when formatCompact isn't enough (e.g. currency).
    valueLabel?: string;
    // Period-over-period change as a percent (12.5 -> +12.5%). Renders a trend badge, except
    // for a flat 0, which carries no trend to show.
    delta?: number;
};

// Every view that shows stat tiles renders them through this section, so the heading,
// its name and the tile grid stay the same on the dashboard, the app overview, the
// conversations view and analytics.
export function StatCardsSection({ cards, action }: { cards: DashboardStatCard[]; action?: ReactNode }) {
    return (
        <SectionCard title="Activity" action={action}>
            <DashboardStatCards cards={cards} />
        </SectionCard>
    );
}

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
                    <span className="flex items-center gap-1 text-sm text-muted-foreground">
                        {card.label}
                        {card.labelInfo && <InfoHint content={card.labelInfo} />}
                    </span>
                    {card.delta !== undefined && card.delta !== 0 && !card.isLoading && (
                        <DeltaBadge delta={card.delta} />
                    )}
                </div>
                {card.isLoading ? (
                    <Skeleton className="h-9 w-20" />
                ) : (
                    <div className="text-3xl font-semibold tracking-tight tabular-nums">{valueLabel}</div>
                )}
                {card.caption && <div className="text-xs text-muted-foreground">{card.caption}</div>}
            </CardContent>
            {card.series && card.series.length > 1 && (
                <Sparkline series={card.series} dates={card.seriesDates} label={card.label} />
            )}
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

const SPARKLINE_DATE_FORMAT = new Intl.DateTimeFormat("en", { month: "short", day: "numeric" });
const SPARKLINE_DATE_TIME_FORMAT = new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
});

function hasTimeOfDay(iso: string): boolean {
    const date = new Date(iso);
    return !Number.isNaN(date.getTime()) && (date.getHours() !== 0 || date.getMinutes() !== 0);
}

function formatSparklineDate(iso: string, showTime: boolean): string {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) {
        return iso;
    }
    return (showTime ? SPARKLINE_DATE_TIME_FORMAT : SPARKLINE_DATE_FORMAT).format(date);
}

function Sparkline({ series, dates, label }: { series: number[]; dates?: string[]; label: string }) {
    const gradientId = `stat-sparkline-${useId().replace(/:/g, "")}`;
    const config = { value: { label, color: "var(--chart-1)" } } satisfies ChartConfig;

    // Pick one date/time format for the whole series so a midnight bucket doesn't read differently.
    const showTime = dates?.some(hasTimeOfDay) ?? false;
    const data = series.map((value, index) => ({
        index,
        value,
        dateLabel: dates?.[index] ? formatSparklineDate(dates[index], showTime) : undefined,
    }));
    const hasDates = data.some((point) => point.dateLabel !== undefined);

    return (
        <ChartContainer config={config} className="aspect-auto h-14 w-full">
            <AreaChart data={data} margin={{ top: 6, right: 0, bottom: 0, left: 0 }}>
                <defs>
                    <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="var(--color-value)" stopOpacity={0.35} />
                        <stop offset="100%" stopColor="var(--color-value)" stopOpacity={0} />
                    </linearGradient>
                </defs>
                <YAxis hide domain={ZERO_SAFE_Y_DOMAIN} />
                <ChartTooltip
                    content={
                        <ChartTooltipContent
                            hideIndicator
                            hideLabel={!hasDates}
                            labelFormatter={(_, payload) => payload?.[0]?.payload?.dateLabel}
                        />
                    }
                />
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
