// Chart series get a color by position from the theme's chart tokens (defined in index.css for
// both light and dark).
const CHART_SERIES_COLORS = ["var(--chart-1)", "var(--chart-2)", "var(--chart-3)", "var(--chart-4)", "var(--chart-5)"];

// Past the fifth series, colors are generated in the same violet-blue family rather than cycling,
// so a sixth series is not a repeat of the first.
//
// The hues are spelled out rather than computed: a 75-degree family holds only about three hues
// that are far enough apart to read as different series, and alternating a lighter and a deeper
// lightness is what doubles that to six. A golden-ratio walk over the same band was tried first
// and put the 6th and 8th series 14 degrees apart at identical lightness. Past six the sequence
// repeats, which is the honest limit of one hue family — a chart needing more than eleven series
// needs a second family, not a finer subdivision of this one.
//
// Both lightnesses sit clear of the band the five defined series occupy, so a generated color is
// never mistaken for one of them, and they come from theme tokens so each theme picks its own.
// Derived from the index rather than actually random: a series has to keep its color across
// renders.
const GENERATED_SERIES = [
    { hue: 252, isDeep: false },
    { hue: 297, isDeep: true },
    { hue: 282, isDeep: false },
    { hue: 267, isDeep: true },
    { hue: 312, isDeep: false },
    { hue: 327, isDeep: true },
];

export function seriesColor(index: number): string {
    if (index < CHART_SERIES_COLORS.length) {
        return CHART_SERIES_COLORS[index];
    }

    const { hue, isDeep } = GENERATED_SERIES[(index - CHART_SERIES_COLORS.length) % GENERATED_SERIES.length];
    const lightness = isDeep ? "var(--chart-extra-l-deep)" : "var(--chart-extra-l)";

    return `oklch(${lightness} var(--chart-extra-c) ${hue})`;
}

// Solid fills for agent avatars, picked deterministically so an agent keeps the same color across
// renders. One lightness so white initials stay legible (>=4.5:1), and a fan across the cool half
// of the wheel only: the warm arc belongs to the coral primary and the crimson destructive, and an
// avatar that lands there reads as a state rather than an identity.
const AGENT_AVATAR_COLORS = ["#558111", "#158561", "#158186", "#147ba9", "#3f69d3", "#735acc", "#964bb4", "#ae4090"];

export function agentAvatarColor(key: string): string {
    let hash = 0;
    for (let index = 0; index < key.length; index++) {
        hash = (hash * 31 + key.charCodeAt(index)) | 0;
    }
    return AGENT_AVATAR_COLORS[Math.abs(hash) % AGENT_AVATAR_COLORS.length];
}
