export type WidgetAppearance = "Light" | "Dark" | "System";

export type WidgetDensity = "Comfortable" | "Compact";

/** Mirrors the server's `WidgetTheme` record. Everything the operator can set, and nothing derived. */
export type WidgetTheme = {
    appearance: WidgetAppearance;
    accentColor: string;
    radius: number;
    fontFamily: string;
    density: WidgetDensity;
    headerTitle: string;
    headerSubtitle: string | null;
    avatarInitials: string | null;
    showHeader: boolean;
    greetingTitle: string | null;
    greetingBody: string | null;
    suggestedPrompts: string[];
    inputPlaceholder: string;
    disclaimer: string | null;
};

export const DEFAULT_ACCENT_COLOR = "#5b4bd6";

export const DEFAULT_THEME: WidgetTheme = {
    appearance: "System",
    accentColor: DEFAULT_ACCENT_COLOR,
    radius: 12,
    fontFamily: 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
    density: "Comfortable",
    headerTitle: "AI Assistant",
    headerSubtitle: "Ask me anything",
    avatarInitials: "AI",
    showHeader: true,
    greetingTitle: "How can I help?",
    greetingBody: "Ask a question and I'll do my best to answer it.",
    suggestedPrompts: [],
    inputPlaceholder: "Ask a question...",
    disclaimer: null,
};

export const MIN_RADIUS = 0;
export const MAX_RADIUS = 24;

type Rgb = { r: number; g: number; b: number };

const HEX_PATTERN = /^#(?:[0-9a-f]{3}|[0-9a-f]{6})$/i;

export function isValidAccentColor(value: string): boolean {
    return HEX_PATTERN.test(value.trim());
}

function parseHex(value: string): Rgb {
    const hex = value.trim().replace("#", "");
    const full = hex.length === 3 ? [...hex].map((digit) => digit + digit).join("") : hex;
    return {
        r: Number.parseInt(full.slice(0, 2), 16),
        g: Number.parseInt(full.slice(2, 4), 16),
        b: Number.parseInt(full.slice(4, 6), 16),
    };
}

function toHex({ r, g, b }: Rgb): string {
    const channel = (value: number) =>
        Math.round(Math.min(255, Math.max(0, value)))
            .toString(16)
            .padStart(2, "0");
    return `#${channel(r)}${channel(g)}${channel(b)}`;
}

/** `amount` is how much of `top` shows through, 0-1. Plain sRGB mixing keeps this cheap and predictable. */
function mix(top: string, bottom: string, amount: number): string {
    const a = parseHex(top);
    const b = parseHex(bottom);
    return toHex({
        r: a.r * amount + b.r * (1 - amount),
        g: a.g * amount + b.g * (1 - amount),
        b: a.b * amount + b.b * (1 - amount),
    });
}

/** WCAG 2.1 relative luminance. */
export function relativeLuminance(color: string): number {
    const { r, g, b } = parseHex(color);
    const linear = (channel: number) => {
        const value = channel / 255;
        return value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
    };
    return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b);
}

export function contrastRatio(foreground: string, background: string): number {
    const a = relativeLuminance(foreground);
    const b = relativeLuminance(background);
    const [lighter, darker] = a > b ? [a, b] : [b, a];
    return (lighter + 0.05) / (darker + 0.05);
}

/** Pure black and white are the only pair that clears 4.5:1 against *every* accent, so the pick is
 *  between those two rather than the softer near-blacks used elsewhere in the palette. */
function readableOn(background: string): string {
    return contrastRatio("#ffffff", background) >= contrastRatio("#000000", background) ? "#ffffff" : "#000000";
}

export type ResolvedAppearance = "Light" | "Dark";

export type WidgetPalette = {
    bg: string;
    surface: string;
    fg: string;
    mutedFg: string;
    border: string;
    accent: string;
    accentFg: string;
    accentHover: string;
    userBubbleBg: string;
    userBubbleFg: string;
    codeBg: string;
    codeBorder: string;
};

const NEUTRALS = {
    Light: { bg: "#ffffff", fg: "#101828", mutedFg: "#596273", ink: "#000000" },
    Dark: { bg: "#0d1117", fg: "#e6e9ef", mutedFg: "#98a2b3", ink: "#ffffff" },
} as const;

/** The single source of truth for every colour the widget paints. The operator picks an accent and an
 *  appearance; the rest is derived here so light and dark stay coherent without eleven swatches. */
export function derivePalette(accentColor: string, appearance: ResolvedAppearance): WidgetPalette {
    const accent = isValidAccentColor(accentColor) ? accentColor.trim().toLowerCase() : DEFAULT_ACCENT_COLOR;
    const { bg, fg, mutedFg, ink } = NEUTRALS[appearance];
    const isDark = appearance === "Dark";

    return {
        bg,
        surface: mix(ink, bg, isDark ? 0.05 : 0.03),
        fg,
        mutedFg,
        border: mix(ink, bg, isDark ? 0.12 : 0.1),
        accent,
        accentFg: readableOn(accent),
        accentHover: mix(isDark ? "#ffffff" : "#000000", accent, 0.12),
        userBubbleBg: mix(accent, bg, isDark ? 0.24 : 0.12),
        userBubbleFg: fg,
        codeBg: mix(ink, bg, isDark ? 0.07 : 0.04),
        codeBorder: mix(ink, bg, isDark ? 0.14 : 0.09),
    };
}

const DENSITY_SCALE = {
    Comfortable: { gap: "1rem", padX: "1rem", padY: "0.875rem", bubblePadY: "0.625rem", lineHeight: "1.6" },
    Compact: { gap: "0.625rem", padX: "0.75rem", padY: "0.625rem", bubblePadY: "0.5rem", lineHeight: "1.5" },
} as const;

export function clampRadius(radius: number): number {
    return Math.min(MAX_RADIUS, Math.max(MIN_RADIUS, Math.round(radius)));
}

/** The CSS custom properties the widget root carries. Every rule in `widget.css` reads from these. */
export function widgetThemeStyle(theme: WidgetTheme, appearance: ResolvedAppearance): Record<string, string> {
    const palette = derivePalette(theme.accentColor, appearance);
    const density = DENSITY_SCALE[theme.density] ?? DENSITY_SCALE.Comfortable;
    const radius = clampRadius(theme.radius);

    return {
        "color-scheme": appearance === "Dark" ? "dark" : "light",
        "--rq-bg": palette.bg,
        "--rq-surface": palette.surface,
        "--rq-fg": palette.fg,
        "--rq-muted-fg": palette.mutedFg,
        "--rq-border": palette.border,
        "--rq-accent": palette.accent,
        "--rq-accent-fg": palette.accentFg,
        "--rq-accent-hover": palette.accentHover,
        "--rq-user-bubble-bg": palette.userBubbleBg,
        "--rq-user-bubble-fg": palette.userBubbleFg,
        "--rq-code-bg": palette.codeBg,
        "--rq-code-border": palette.codeBorder,
        "--rq-radius": `${radius}px`,
        "--rq-radius-sm": `${Math.max(2, Math.round(radius * 0.5))}px`,
        "--rq-radius-pill": `${Math.max(8, radius + 12)}px`,
        "--rq-font": theme.fontFamily,
        "--rq-gap": density.gap,
        "--rq-pad-x": density.padX,
        "--rq-pad-y": density.padY,
        "--rq-bubble-pad-y": density.bubblePadY,
        "--rq-line-height": density.lineHeight,
    };
}
