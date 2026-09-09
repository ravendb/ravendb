import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fireEvent, waitFor } from "storybook/test";
import { WritesBarChart } from "./charts";

const DATA = Array.from({ length: 12 }, (_, index) => ({
    t: `2026-${String(index + 1).padStart(2, "0")}`,
    writes: 100 + index * 25,
}));

// Tests, not catalogue entries: `!dev` keeps these out of the Storybook sidebar while
// `pnpm test:storybook` still runs them. Play functions are the only way to exercise a
// component here — the unit project runs in node with no DOM.
const meta = {
    title: "Data/Charts",
    component: WritesBarChart,
    tags: ["!dev"],
    args: { data: DATA, xKey: "t" },
} satisfies Meta<typeof WritesBarChart>;

export default meta;

type Story = StoryObj<typeof meta>;

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));

function getChart(canvasElement: HTMLElement) {
    const chart = canvasElement.querySelector<HTMLElement>(".recharts-wrapper");
    if (!chart) throw new Error("chart did not render");
    return chart;
}

function getTooltip(chart: HTMLElement) {
    const tooltip = chart.querySelector<HTMLElement>(".recharts-tooltip-wrapper");
    if (!tooltip) throw new Error("tooltip wrapper did not render");
    return tooltip;
}

// Recharts positions the tooltip by transform, so the assertions below read the inline style
// rather than a rendered offset. Step frames explicitly instead of using waitFor: the whole
// point is what the tooltip looks like on the frame it appears, and waitFor's polling interval
// is long enough to skip straight past it.
async function pointAtAndWaitForTooltip(chart: HTMLElement, horizontalFraction: number) {
    const tooltip = getTooltip(chart);
    const box = chart.getBoundingClientRect();
    const pointerOffset = box.width * horizontalFraction;

    fireEvent.mouseMove(chart, { clientX: box.left + pointerOffset, clientY: box.top + box.height / 2 });

    for (let frames = 0; frames < 10 && tooltip.style.visibility !== "visible"; frames++) {
        await nextFrame();
    }
    expect(tooltip).toBeVisible();

    return { tooltip, pointerOffset };
}

// React derives onMouseLeave from the native mouseout, so a dispatched mouseleave — which does
// not bubble — never reaches recharts.
function leave(chart: HTMLElement) {
    fireEvent.mouseOut(chart, { relatedTarget: document.body });
}

function getTranslateX(tooltip: HTMLElement) {
    const match = /translate\((-?[\d.]+)px/.exec(tooltip.style.transform);
    if (!match?.[1]) throw new Error(`tooltip is not positioned by translate: "${tooltip.style.transform}"`);
    return Number(match[1]);
}

export const TooltipAppearsAtThePointerWithoutSlidingIn: Story = {
    play: async ({ canvasElement }) => {
        const chart = await waitFor(() => getChart(canvasElement));
        const { tooltip, pointerOffset } = await pointAtAndWaitForTooltip(chart, 0.3);

        // No transition on the frame it appears, so it paints where it belongs instead of
        // animating in from the chart origin. This is the regression the audit reported.
        expect(tooltip.style.transition).toBe("");

        // And where it belongs is next to the pointer. The coordinate snaps to the hovered
        // bucket, hence the tolerance; the origin would be a whole pointerOffset away.
        const appearedAt = getTranslateX(tooltip);
        expect(Math.abs(appearedAt - pointerOffset)).toBeLessThan(80);

        // Once it has appeared the transition comes back, so moving on glides rather than jumps.
        await waitFor(() => expect(tooltip.style.transition).toContain("transform"));

        // Settling must not shift it — the glide is for the next move, not for arriving.
        expect(getTranslateX(tooltip)).toBe(appearedAt);
    },
};

// Why the audit filed this globally: with several charts on a page you enter one after another,
// and every entry replayed the slide-in.
export const ReEnteringAChartDoesNotSlideIn: Story = {
    play: async ({ canvasElement }) => {
        const chart = await waitFor(() => getChart(canvasElement));

        const first = await pointAtAndWaitForTooltip(chart, 0.3);
        await waitFor(() => expect(first.tooltip.style.transition).toContain("transform"));

        leave(chart);
        await waitFor(() => expect(first.tooltip).not.toBeVisible());

        const second = await pointAtAndWaitForTooltip(chart, 0.75);
        expect(second.tooltip.style.transition).toBe("");
        expect(Math.abs(getTranslateX(second.tooltip) - second.pointerOffset)).toBeLessThan(80);
    },
};
