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
import { seriesColor } from "@/lib/palette";

const writesChartConfig = {
    writes: { label: "Writes", color: "var(--chart-1)" },
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

// Single-series writes bar chart shared by the Usage page ("Writes this month") and the
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
                <BarChart accessibilityLayer data={data} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey={xKey} tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                    <YAxis hide domain={ZERO_SAFE_Y_DOMAIN} />
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
    const config: ChartConfig = Object.fromEntries(
        data.keys.map((series, index): [string, { label: string; color: string }] => [
            series.key,
            { label: series.label, color: seriesColor(index) },
        ]),
    );

    return (
        <div ref={ref}>
            <ChartContainer config={config} className="aspect-auto h-56 w-full">
                <BarChart accessibilityLayer data={data.points} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="t" tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                    <YAxis hide domain={ZERO_SAFE_Y_DOMAIN} />
                    <ChartTooltip content={<ChartTooltipContent />} />
                    <ChartLegend content={<ChartLegendContent />} />
                    {data.keys.map((series, index) => (
                        <Bar
                            key={series.key}
                            dataKey={series.key}
                            stackId="series"
                            fill={seriesColor(index)}
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
