import { useRef } from "react";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import type { SeriesData } from "@/api/generated/server-api";
import {
    ChartContainer,
    ChartLegend,
    ChartLegendContent,
    ChartTooltip,
    ChartTooltipContent,
    type ChartConfig,
} from "@/components/shadcn/ui/chart";
import { ZERO_SAFE_Y_DOMAIN } from "@/lib/chart-domain";
import { formatCompact } from "@/lib/format";
import { seriesColor } from "@/lib/palette";

const writesChartConfig = {
    writes: { label: "WRU", color: "var(--chart-1)" },
} satisfies ChartConfig;

// Zooms the chart in from a clicked bar. Played imperatively (not via a keyed
// remount) so it replays on every click without the chart blanking out first.
function useZoomOnClick() {
    const ref = useRef<HTMLDivElement>(null);
    const zoomFrom = (index: number, count: number) => {
        const chart = ref.current;
        if (!chart) return;
        chart.style.transformOrigin = `${((index + 0.5) / count) * 100}% bottom`;
        chart.animate(
            [
                { opacity: 0, transform: "scale(0.9)" },
                { opacity: 1, transform: "scale(1)" },
            ],
            { duration: 300, easing: "ease-out" },
        );
    };
    return { ref, zoomFrom };
}

// Single-series writes bar chart shared by the Usage page ("WRU" card) and the
// per-app CDC writes section, which differ only in their x-axis key. Passing `onBarClick`
// makes the bars clickable and receives the clicked bucket, used to drill a period from
// year into month into day; the chart then zooms in from the clicked bar.
export function WritesBarChart({
    data,
    xKey,
    onBarClick,
}: {
    data: Array<Record<string, unknown>>;
    xKey: string;
    onBarClick?: (entry: Record<string, unknown>) => void;
}) {
    const { ref, zoomFrom } = useZoomOnClick();

    return (
        <div ref={ref}>
            <ChartContainer config={writesChartConfig} className="aspect-auto h-56 w-full">
                <BarChart
                    accessibilityLayer
                    data={data} // right margin leaves room for the last x-axis label, which recharts centers on the
                    // final bucket and would otherwise clip against the chart's right edge.
                    margin={{ top: 8, right: 32, bottom: 0, left: 0 }}
                >
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey={xKey} tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                    <YAxis
                        domain={ZERO_SAFE_Y_DOMAIN}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        width={44}
                        tickFormatter={(value) => formatCompact(value as number)}
                    />
                    <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
                    <Bar
                        dataKey="writes"
                        fill="var(--color-writes)"
                        radius={[4, 4, 0, 0]}
                        className={onBarClick ? "cursor-pointer" : undefined}
                        onClick={
                            onBarClick
                                ? (bar, index) => {
                                      zoomFrom(index, data.length);
                                      onBarClick(bar.payload);
                                  }
                                : undefined
                        }
                        // The drill-down chart zooms on click, so bars update in place rather
                        // than replaying the grow-in that would read as a blank-and-redraw.
                        isAnimationActive={!onBarClick}
                    />
                </BarChart>
            </ChartContainer>
        </div>
    );
}

// Multi-series stacked bar chart for the App Usage breakdowns (tokens by capability /
// model, conversations by channel). Each point is shaped { t, <key>: number, ... } and
// `keys` names and labels each series — the chart joins on key, colors by position from
// the local palette, and renders the label. Passing `onBarClick` makes a column drill the
// period from the clicked bucket, zooming in from that column (see WritesBarChart).
export function SeriesBarChart({
    data,
    onBarClick,
}: {
    data: SeriesData;
    onBarClick?: (entry: Record<string, unknown>) => void;
}) {
    const { ref, zoomFrom } = useZoomOnClick();

    // Color by original index so a series keeps its color regardless of which others are
    // present, then drop any series that is zero across the whole visible period — it adds
    // nothing to the chart and would only pad the legend.
    const visibleSeries = data.keys
        .map((series, index) => ({ ...series, color: seriesColor(index) }))
        .filter((series) => data.points.some((point) => Number(point[series.key]) > 0));

    const config: ChartConfig = Object.fromEntries(
        visibleSeries.map((series): [string, { label: string; color: string }] => [
            series.key,
            { label: series.label, color: series.color },
        ]),
    );

    return (
        <div ref={ref}>
            <ChartContainer config={config} className="aspect-auto h-56 w-full">
                <BarChart
                    accessibilityLayer
                    data={data.points} // right margin leaves room for the last x-axis label, which recharts centers on the
                    // final bucket and would otherwise clip against the chart's right edge.
                    margin={{ top: 8, right: 32, bottom: 0, left: 0 }}
                >
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="t" tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                    <YAxis
                        domain={ZERO_SAFE_Y_DOMAIN}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        width={44}
                        tickFormatter={(value) => formatCompact(value as number)}
                    />
                    <ChartTooltip content={<ChartTooltipContent hideZero />} />
                    <ChartLegend content={<ChartLegendContent />} />
                    {visibleSeries.map((series) => (
                        <Bar
                            key={series.key}
                            dataKey={series.key}
                            stackId="series"
                            fill={series.color}
                            className={onBarClick ? "cursor-pointer" : undefined}
                            onClick={
                                onBarClick
                                    ? (bar, barIndex) => {
                                          zoomFrom(barIndex, data.points.length);
                                          onBarClick(bar.payload);
                                      }
                                    : undefined
                            }
                            isAnimationActive={!onBarClick}
                        />
                    ))}
                </BarChart>
            </ChartContainer>
        </div>
    );
}
