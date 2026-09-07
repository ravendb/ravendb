import { type ReactNode, useRef } from "react";
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

// Shared bar-chart scaffolding for WritesBarChart and SeriesBarChart: the zoom-on-click
// wrapper, chart container, grid, and both axes. Children supply the tooltip, legend, and
// bars, which differ between the two charts, and receive `zoomFrom(barIndex)` for bar clicks.
function BarChartFrame({
    config,
    data,
    xKey,
    xTickFormatter,
    children,
}: {
    config: ChartConfig;
    data: Array<Record<string, unknown>>;
    xKey: string;
    xTickFormatter?: (value: string) => string;
    children: (zoomFrom: (barIndex: number) => void) => ReactNode;
}) {
    const { ref, zoomFrom } = useZoomOnClick();

    return (
        <div ref={ref}>
            <ChartContainer config={config} className="aspect-auto h-56 w-full">
                <BarChart
                    accessibilityLayer
                    data={data}
                    // right margin leaves room for the last x-axis label, which recharts centers on the
                    // final bucket and would otherwise clip against the chart's right edge.
                    margin={{ top: 8, right: 32, bottom: 0, left: 0 }}
                >
                    <CartesianGrid vertical={false} />
                    <XAxis
                        dataKey={xKey}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        interval="equidistantPreserveStart"
                        minTickGap={16}
                        tickFormatter={xTickFormatter && ((value) => xTickFormatter(value as string))}
                    />
                    <YAxis
                        domain={ZERO_SAFE_Y_DOMAIN}
                        allowDecimals={false}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        // Size to the labels so large values (e.g. "429.1K") aren't clipped.
                        width="auto"
                        tickFormatter={(value) => formatCompact(value as number)}
                    />
                    {children((barIndex) => zoomFrom(barIndex, data.length))}
                </BarChart>
            </ChartContainer>
        </div>
    );
}

// Single-series writes bar chart shared by the Usage page ("WRU" card) and the
// per-app CDC writes section, which differ only in their x-axis key. Passing `onBarClick`
// makes the bars clickable and receives the clicked bucket, used to drill a period from
// year into month into day; the chart then zooms in from the clicked bar.
export function WritesBarChart({
    data,
    xKey,
    xTickFormatter,
    tooltipLabelFormatter,
    onBarClick,
}: {
    data: Array<Record<string, unknown>>;
    xKey: string;
    xTickFormatter?: (value: string) => string;
    tooltipLabelFormatter?: (value: string) => string;
    onBarClick?: (entry: Record<string, unknown>) => void;
}) {
    return (
        <BarChartFrame config={writesChartConfig} data={data} xKey={xKey} xTickFormatter={xTickFormatter}>
            {(zoomFrom) => (
                <>
                    <ChartTooltip
                        cursor={false}
                        content={
                            <ChartTooltipContent
                                labelFormatter={
                                    tooltipLabelFormatter
                                        ? (value) => tooltipLabelFormatter(value as string)
                                        : undefined
                                }
                            />
                        }
                    />
                    <Bar
                        dataKey="writes"
                        fill="var(--color-writes)"
                        radius={[4, 4, 0, 0]}
                        className={onBarClick ? "cursor-pointer" : undefined}
                        onClick={
                            onBarClick
                                ? (bar, index) => {
                                      zoomFrom(index);
                                      onBarClick(bar.payload);
                                  }
                                : undefined
                        }
                        // The drill-down chart zooms on click, so bars update in place rather
                        // than replaying the grow-in that would read as a blank-and-redraw.
                        isAnimationActive={!onBarClick}
                    />
                </>
            )}
        </BarChartFrame>
    );
}

// Multi-series stacked bar chart for the App Usage breakdowns (tokens by capability /
// model, conversations by channel). Each point is shaped { t, <key>: number, ... } and
// `keys` names and labels each series — the chart joins on key, colors by position from
// the local palette, and renders the label. Passing `onBarClick` makes a column drill the
// period from the clicked bucket, zooming in from that column (see WritesBarChart).
export function SeriesBarChart({
    data,
    xTickFormatter,
    tooltipLabelFormatter,
    onBarClick,
}: {
    data: SeriesData;
    xTickFormatter?: (value: string) => string;
    tooltipLabelFormatter?: (value: string) => string;
    onBarClick?: (entry: Record<string, unknown>) => void;
}) {
    // Color by original index so a series keeps its color regardless of which others are present.
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
        <BarChartFrame config={config} data={data.points} xKey="t" xTickFormatter={xTickFormatter}>
            {(zoomFrom) => (
                <>
                    <ChartTooltip
                        content={
                            <ChartTooltipContent
                                hideZero
                                labelFormatter={
                                    tooltipLabelFormatter
                                        ? (value) => tooltipLabelFormatter(value as string)
                                        : undefined
                                }
                            />
                        }
                    />
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
                                          zoomFrom(barIndex);
                                          onBarClick(bar.payload);
                                      }
                                    : undefined
                            }
                            isAnimationActive={!onBarClick}
                        />
                    ))}
                </>
            )}
        </BarChartFrame>
    );
}
