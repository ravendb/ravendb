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

const writesChartConfig = {
    writes: { label: "Writes", color: "var(--chart-1)" },
} satisfies ChartConfig;

// Single-series writes bar chart shared by the Usage page ("Writes this month") and the
// per-app CDC writes section, which differ only in their x-axis key.
export function WritesBarChart({ data, xKey }: { data: Array<Record<string, unknown>>; xKey: string }) {
    return (
        <ChartContainer config={writesChartConfig} className="aspect-auto h-56 w-full">
            <BarChart accessibilityLayer data={data} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey={xKey} tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                <YAxis hide domain={[0, "dataMax"]} />
                <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
                <Bar dataKey="writes" fill="var(--color-writes)" radius={[4, 4, 0, 0]} />
            </BarChart>
        </ChartContainer>
    );
}

// Multi-series stacked bar chart for the App Usage breakdowns (tokens by capability /
// model, conversations by channel). Each point is shaped { t, <key>: number, ... } and
// `keys` names, labels and colors each series — the chart joins on key and renders label.
export function SeriesBarChart({ data }: { data: SeriesData }) {
    const config: ChartConfig = Object.fromEntries(
        data.keys.map((series): [string, { label: string; color: string }] => [
            series.key,
            { label: series.label, color: series.color },
        ]),
    );

    return (
        <ChartContainer config={config} className="aspect-auto h-56 w-full">
            <BarChart accessibilityLayer data={data.points} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey="t" tickLine={false} axisLine={false} tickMargin={8} interval={2} />
                <YAxis hide domain={[0, "dataMax"]} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <ChartLegend content={<ChartLegendContent />} />
                {data.keys.map((series) => (
                    <Bar key={series.key} dataKey={series.key} stackId="series" fill={series.color} />
                ))}
            </BarChart>
        </ChartContainer>
    );
}
