export type WidgetAppearance = "Light" | "Dark" | "System";

export type WidgetRadius = "None" | "Small" | "Medium" | "Large";

export type WidgetFontSize = "Small" | "Medium" | "Large" | "Custom";

/** The logo's own rounding, independent of `WidgetRadius`: logos are usually avatars, so `Pill` is the default. */
export type WidgetLogoRadius = "None" | "Small" | "Medium" | "Large" | "Pill";

/** The colors an operator picks for one scheme; everything else the widget paints is derived from them. */
export type WidgetThemeColors = {
    /** Buttons, links and highlights; the widget's accent. */
    buttonColor: string;
    /** The visitor's message bubbles. */
    messageColor: string;
    backgroundColor: string;
};

/** Mirrors the server's `WidgetTheme` record. Everything the operator can set, and nothing derived.
 *  Colors are per scheme; `appearance` picks the default scheme, and the host page can override it per
 *  visitor. Everything else is shared between the schemes. */
export type WidgetTheme = {
    appearance: WidgetAppearance;
    light: WidgetThemeColors;
    dark: WidgetThemeColors;
    radius: WidgetRadius;
    fontFamily: string;
    fontSize: WidgetFontSize;
    /** Only read when `fontSize` is `Custom`; clamped to 0.625-1.5. */
    customFontSizeRem: number | null;
    /** A `data:image/...` URI shown in the header; the embed CSP allows `img-src data:`. */
    logo: string | null;
    logoRadius: WidgetLogoRadius;
    headerTitle: string;
    headerSubtitle: string | null;
    showHeader: boolean;
    greetingTitle: string | null;
    greetingBody: string | null;
    suggestedPrompts: string[];
    inputPlaceholder: string;
    disclaimer: string | null;
    /** Appended after the widget's own styles; in live mode the server shell injects it with a CSP nonce. */
    customCss: string | null;
};

export const DEFAULT_ACCENT_COLOR = "#ff775f";

export const DEFAULT_THEME: WidgetTheme = {
    appearance: "System",
    // The message colors are the button color mixed into the background (12% light, 24% dark), the mix the
    // palette derived on its own before the message color became an operator option.
    light: { buttonColor: DEFAULT_ACCENT_COLOR, messageColor: "#ffefec", backgroundColor: "#ffffff" },
    dark: { buttonColor: DEFAULT_ACCENT_COLOR, messageColor: "#472928", backgroundColor: "#0d1117" },
    radius: "Medium",
    fontFamily: 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
    fontSize: "Medium",
    customFontSizeRem: null,
    logo: null,
    logoRadius: "Pill",
    headerTitle: "AI Assistant",
    headerSubtitle: "Ask me anything",
    showHeader: true,
    greetingTitle: "How can I help?",
    greetingBody: "Ask a question and I'll do my best to answer it.",
    suggestedPrompts: [],
    inputPlaceholder: "Ask a question...",
    disclaimer: null,
    customCss: null,
};

type Rgb = { r: number; g: number; b: number };

const HEX_PATTERN = /^#(?:[0-9a-f]{3}|[0-9a-f]{6})$/i;

export function isValidHexColor(value: string): boolean {
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

const WCAG_AA_NORMAL_TEXT = 4.5;

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
    /** Follows the background the widget actually paints, so native scrollbars and controls match it. */
    colorScheme: "light" | "dark";
};

const NEUTRALS = {
    Light: { bg: "#ffffff", fg: "#101828", mutedFg: "#596273", ink: "#000000" },
    Dark: { bg: "#0d1117", fg: "#e6e9ef", mutedFg: "#98a2b3", ink: "#ffffff" },
} as const;

type Neutrals = { bg: string; fg: string; mutedFg: string; ink: string; isDark: boolean };

/** The most faded mix of `fg` into `bg` that still clears WCAG AA, so muted text stays muted without
 *  becoming unreadable on whatever background the operator picked. */
function mutedOn(fg: string, bg: string): string {
    for (const amount of [0.62, 0.72, 0.84]) {
        const candidate = mix(fg, bg, amount);
        if (contrastRatio(candidate, bg) >= WCAG_AA_NORMAL_TEXT) return candidate;
    }
    return fg;
}

/** A changed background replaces the whole neutral ramp: its own luminance decides whether the widget
 *  reads as light or dark, independent of the appearance the operator picked. The scheme's own default
 *  background keeps the curated ramp rather than a recomputed approximation of it. */
function neutralsFor(appearance: ResolvedAppearance, backgroundColor: string): Neutrals {
    const scheme = NEUTRALS[appearance];
    const custom = isValidHexColor(backgroundColor) ? backgroundColor.trim().toLowerCase() : scheme.bg;

    if (custom === scheme.bg) {
        const { bg, fg, mutedFg, ink } = scheme;
        return { bg, fg, mutedFg, ink, isDark: appearance === "Dark" };
    }

    const ink = readableOn(custom);
    const isDark = ink === "#ffffff";
    // The softened default foreground where it still clears AA; pure ink on backgrounds too mid-toned for it.
    const softFg = isDark ? NEUTRALS.Dark.fg : NEUTRALS.Light.fg;
    const fg = contrastRatio(softFg, custom) >= WCAG_AA_NORMAL_TEXT ? softFg : ink;
    return { bg: custom, fg, mutedFg: mutedOn(fg, custom), ink, isDark };
}

/** The single source of truth for every color the widget paints. The operator picks a button, message and
 *  background color per scheme; the rest is derived here so light and dark stay coherent without eleven
 *  swatches. An invalid color falls back rather than failing: the button color to the default accent, the
 *  background to the scheme's own, and the message color to a mix of the other two. */
export function derivePalette(colors: WidgetThemeColors, appearance: ResolvedAppearance): WidgetPalette {
    const accent = isValidHexColor(colors.buttonColor) ? colors.buttonColor.trim().toLowerCase() : DEFAULT_ACCENT_COLOR;
    const { bg, fg, mutedFg, ink, isDark } = neutralsFor(appearance, colors.backgroundColor);

    const userBubbleBg = isValidHexColor(colors.messageColor)
        ? colors.messageColor.trim().toLowerCase()
        : mix(accent, bg, isDark ? 0.24 : 0.12);

    return {
        bg,
        surface: mix(ink, bg, isDark ? 0.05 : 0.03),
        fg,
        mutedFg,
        border: mix(ink, bg, isDark ? 0.12 : 0.1),
        accent,
        accentFg: readableOn(accent),
        accentHover: mix(isDark ? "#ffffff" : "#000000", accent, 0.12),
        userBubbleBg,
        userBubbleFg: contrastRatio(fg, userBubbleBg) >= WCAG_AA_NORMAL_TEXT ? fg : readableOn(userBubbleBg),
        codeBg: mix(ink, bg, isDark ? 0.07 : 0.04),
        codeBorder: mix(ink, bg, isDark ? 0.14 : 0.09),
        colorScheme: isDark ? "dark" : "light",
    };
}

/** One named size rounds every corner the widget draws: surfaces, small chrome, and the pill-shaped
 *  composer and prompt buttons. `None` really means none - the pills go square too. The base value is
 *  mirrored by `RadiusPx` in `WidgetShell.cs` for the pre-bundle first paint. */
export const RADIUS_SCALE: Record<WidgetRadius, { radius: string; sm: string; pill: string }> = {
    None: { radius: "0px", sm: "0px", pill: "0px" },
    Small: { radius: "6px", sm: "4px", pill: "10px" },
    Medium: { radius: "12px", sm: "6px", pill: "20px" },
    Large: { radius: "18px", sm: "10px", pill: "9999px" },
};

/** The logo's rounding. `Pill` uses 100vh rather than a percentage so a non-square logo gets pill-shaped
 *  sides instead of an ellipse. */
export const LOGO_RADIUS_SCALE: Record<WidgetLogoRadius, string> = {
    None: "0px",
    Small: "4px",
    Medium: "8px",
    Large: "12px",
    Pill: "100vh",
};

const SPACING = {
    gap: "1rem",
    padX: "1rem",
    padY: "0.875rem",
    bubblePadY: "0.625rem",
    lineHeight: "1.6",
} as const;

/** Base rem for the named sizes; `Custom` reads the operator's own value. Mirrored by `FontSizeRem` in
 *  `WidgetShell.cs` for the pre-bundle first paint. */
const FONT_SIZE_REM: Record<Exclude<WidgetFontSize, "Custom">, number> = {
    Small: 0.875,
    Medium: 1,
    Large: 1.125,
};

export const MIN_CUSTOM_FONT_SIZE_REM = 0.625;
export const MAX_CUSTOM_FONT_SIZE_REM = 1.5;

/** Applied to the document root (`html`), so every rem-based size in the widget scales with it. */
export function resolveFontSizeRem(theme: WidgetTheme): number {
    if (theme.fontSize === "Custom") {
        const rem = theme.customFontSizeRem ?? 1;
        return Math.min(MAX_CUSTOM_FONT_SIZE_REM, Math.max(MIN_CUSTOM_FONT_SIZE_REM, rem));
    }
    return FONT_SIZE_REM[theme.fontSize] ?? 1;
}

/** The CSS custom properties the widget root carries. Every rule in `widget.css` reads from these. */
export function widgetThemeStyle(theme: WidgetTheme, appearance: ResolvedAppearance): Record<string, string> {
    const colors =
        (appearance === "Dark" ? theme.dark : theme.light) ??
        (appearance === "Dark" ? DEFAULT_THEME.dark : DEFAULT_THEME.light);
    const palette = derivePalette(colors, appearance);
    const radius = RADIUS_SCALE[theme.radius] ?? RADIUS_SCALE.Medium;

    return {
        "color-scheme": palette.colorScheme,
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
        "--rq-radius": radius.radius,
        "--rq-radius-sm": radius.sm,
        "--rq-radius-pill": radius.pill,
        "--rq-logo-radius": LOGO_RADIUS_SCALE[theme.logoRadius] ?? LOGO_RADIUS_SCALE.Pill,
        "--rq-font": theme.fontFamily,
        "--rq-gap": SPACING.gap,
        "--rq-pad-x": SPACING.padX,
        "--rq-pad-y": SPACING.padY,
        "--rq-bubble-pad-y": SPACING.bubblePadY,
        "--rq-line-height": SPACING.lineHeight,
    };
}
