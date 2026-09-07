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

// Fades the chart in after a bar click. Played imperatively (not via a keyed remount) so
// it replays on every click without the chart blanking out first. Deliberately no scale:
// the y-axis "auto" width is measured from the rendered labels, and a scaled-down chart
// would be measured too narrow, clipping the top label.
function useFadeInOnClick() {
    const ref = useRef<HTMLDivElement>(null);
    const fadeIn = () => {
        ref.current?.animate(
            [
                { opacity: 0, transform: "translateY(8px)" },
                { opacity: 1, transform: "translateY(0)" },
            ],
            { duration: 300, easing: "ease-out" },
        );
    };
    return { ref, fadeIn };
}

// Shared bar-chart scaffolding for WritesBarChart and SeriesBarChart: the fade-in-on-click
// wrapper, chart container, grid, and both axes. Children supply the tooltip, legend, and
// bars, which differ between the two charts, and receive `fadeIn()` for bar clicks.
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
    children: (fadeIn: () => void) => ReactNode;
}) {
    const { ref, fadeIn } = useFadeInOnClick();

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
                    {children(fadeIn)}
                </BarChart>
            </ChartContainer>
        </div>
    );
}

// Single-series writes bar chart shared by the Usage page ("WRU" card) and the
// per-app CDC writes section, which differ only in their x-axis key. Passing `onBarClick`
// makes the bars clickable and receives the clicked bucket, used to drill a period from
// year into month into day; the chart then fades in with the new data.
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
            {(fadeIn) => (
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
                                ? (bar) => {
                                      fadeIn();
                                      onBarClick(bar.payload);
                                  }
                                : undefined
                        }
                        // The drill-down chart fades in on click, so bars update in place rather
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
// period from the clicked bucket, fading in the new data (see WritesBarChart).
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
            {(fadeIn) => (
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
                                    ? (bar) => {
                                          fadeIn();
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
