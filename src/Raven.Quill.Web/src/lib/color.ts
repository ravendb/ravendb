export type Rgb = { r: number; g: number; b: number };
export type Hsl = { h: number; s: number; l: number };
export type ColorFormat = "hex" | "rgb" | "hsl";

const HEX_PATTERN = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i;
/** rgb(a, b, c), hsl(a, b%, c%), or the same three numbers bare, comma or space separated. */
const TRIPLE_PATTERN =
    /^(rgb|hsl)?\s*\(?\s*(-?[\d.]+)\s*(%?)\s*[,\s]\s*(-?[\d.]+)\s*(%?)\s*[,\s]\s*(-?[\d.]+)\s*(%?)\s*\)?$/i;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

function fromHex(text: string): Rgb {
    const digits = text.slice(1);
    const full = digits.length === 3 ? [...digits].map((digit) => digit + digit).join("") : digits;
    return {
        r: parseInt(full.slice(0, 2), 16),
        g: parseInt(full.slice(2, 4), 16),
        b: parseInt(full.slice(4, 6), 16),
    };
}

/**
 * Reads any of the three formats the picker offers. `assume` disambiguates a bare triple, which is
 * otherwise unreadable: "150, 41, 31" is a different colour depending on which tab typed it. An
 * explicit function name or a percent sign always wins over the hint.
 */
export function parseColor(text: string, assume: ColorFormat = "rgb"): Rgb | null {
    const input = text.trim();
    if (HEX_PATTERN.test(input)) return fromHex(input);

    const match = TRIPLE_PATTERN.exec(input);
    if (!match) return null;

    const [, fn, first, firstUnit, second, secondUnit, third, thirdUnit] = match;
    const values = [Number(first), Number(second), Number(third)];
    if (values.some((value) => Number.isNaN(value))) return null;

    const hasPercent = Boolean(firstUnit || secondUnit || thirdUnit);
    const isHsl = fn?.toLowerCase() === "hsl" || (!fn && hasPercent) || (!fn && !hasPercent && assume === "hsl");

    if (isHsl) return hslToRgb({ h: values[0], s: values[1], l: values[2] });
    return {
        r: clamp(Math.round(values[0]), 0, 255),
        g: clamp(Math.round(values[1]), 0, 255),
        b: clamp(Math.round(values[2]), 0, 255),
    };
}

export function toHex({ r, g, b }: Rgb): string {
    return `#${[r, g, b].map((channel) => clamp(Math.round(channel), 0, 255).toString(16).padStart(2, "0")).join("")}`;
}

export function rgbToHsl({ r, g, b }: Rgb): Hsl {
    const [red, green, blue] = [r / 255, g / 255, b / 255];
    const max = Math.max(red, green, blue);
    const min = Math.min(red, green, blue);
    const lightness = (max + min) / 2;
    const chroma = max - min;

    if (chroma === 0) return { h: 0, s: 0, l: Math.round(lightness * 100) };

    const saturation = chroma / (1 - Math.abs(2 * lightness - 1));
    const sector =
        max === red
            ? ((green - blue) / chroma) % 6
            : max === green
              ? (blue - red) / chroma + 2
              : (red - green) / chroma + 4;
    const hue = Math.round(sector * 60);

    return {
        h: hue < 0 ? hue + 360 : hue,
        s: Math.round(saturation * 100),
        l: Math.round(lightness * 100),
    };
}

export function hslToRgb({ h, s, l }: Hsl): Rgb {
    const hue = ((h % 360) + 360) % 360;
    const saturation = clamp(s, 0, 100) / 100;
    const lightness = clamp(l, 0, 100) / 100;

    const chroma = (1 - Math.abs(2 * lightness - 1)) * saturation;
    const second = chroma * (1 - Math.abs(((hue / 60) % 2) - 1));
    const base = lightness - chroma / 2;

    const [red, green, blue] =
        hue < 60
            ? [chroma, second, 0]
            : hue < 120
              ? [second, chroma, 0]
              : hue < 180
                ? [0, chroma, second]
                : hue < 240
                  ? [0, second, chroma]
                  : hue < 300
                    ? [second, 0, chroma]
                    : [chroma, 0, second];

    return {
        r: Math.round((red + base) * 255),
        g: Math.round((green + base) * 255),
        b: Math.round((blue + base) * 255),
    };
}

/** The display string for a hex value in the tab the operator is looking at. */
export function formatAs(format: ColorFormat, hex: string): string {
    const rgb = parseColor(hex);
    if (!rgb) return hex;
    if (format === "hex") return toHex(rgb);
    if (format === "rgb") return `${rgb.r}, ${rgb.g}, ${rgb.b}`;
    const { h, s, l } = rgbToHsl(rgb);
    return `${h}, ${s}%, ${l}%`;
}
