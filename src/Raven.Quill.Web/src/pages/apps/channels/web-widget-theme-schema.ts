import { z } from "zod";
import type { WidgetTheme } from "@/api/generated/server-api";

// Mirrors WidgetThemeValidation.cs. The server is still the authority; these bounds exist so the operator
// gets the message next to the field instead of after a round trip.
const MAX_RADIUS = 24;
const MAX_SUGGESTED_PROMPTS = 4;
const MAX_SUGGESTED_PROMPT_LENGTH = 80;

const HEX_COLOR = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

// Everything a font stack legitimately needs; rules out `;`, `{`, `}`, `@` and `url(` by construction.
const FONT_STACK = /^[A-Za-z0-9 ,'"-]+$/;

const optionalText = (max: number, label: string) =>
    z
        .string()
        .max(max, `${label} must be ${max} characters or fewer`)
        .transform((value) => value.trim())
        .nullable();

export const widgetThemeSchema = z.object({
    appearance: z.enum(["Light", "Dark", "System"]),
    accentColor: z
        .string()
        .trim()
        .regex(HEX_COLOR, "Enter a hex colour such as #2f6f4f")
        .transform((value) => value.toLowerCase()),
    radius: z.number().int().min(0).max(MAX_RADIUS),
    fontFamily: z
        .string()
        .trim()
        .min(1, "Pick a font")
        .max(200, "Font stack must be 200 characters or fewer")
        .regex(FONT_STACK, "A font stack may only contain letters, digits, spaces, commas, hyphens and quotes"),
    density: z.enum(["Comfortable", "Compact"]),
    headerTitle: z.string().trim().min(1, "A title is required").max(60, "Title must be 60 characters or fewer"),
    headerSubtitle: optionalText(100, "Subtitle"),
    avatarInitials: optionalText(3, "Initials"),
    showHeader: z.boolean(),
    greetingTitle: optionalText(80, "Greeting title"),
    greetingBody: optionalText(240, "Greeting body"),
    suggestedPrompts: z
        .array(
            z.object({
                value: z
                    .string()
                    .trim()
                    .max(
                        MAX_SUGGESTED_PROMPT_LENGTH,
                        `Each prompt must be ${MAX_SUGGESTED_PROMPT_LENGTH} characters or fewer`,
                    ),
            }),
        )
        .max(MAX_SUGGESTED_PROMPTS, `Up to ${MAX_SUGGESTED_PROMPTS} prompts`),
    inputPlaceholder: z
        .string()
        .trim()
        .min(1, "A placeholder is required")
        .max(80, "Placeholder must be 80 characters or fewer"),
    disclaimer: optionalText(200, "Disclaimer"),
});

/** The form's shape. `suggestedPrompts` is a list of objects because `useFieldArray` needs stable keys. */
export type WidgetThemeFormData = z.input<typeof widgetThemeSchema>;

/** What the resolver hands to `handleSubmit`: trimmed and normalized, but still the form's prompt shape. */
export type WidgetThemeFormOutput = z.output<typeof widgetThemeSchema>;

export const MAX_PROMPTS = MAX_SUGGESTED_PROMPTS;
export const RADIUS_MAX = MAX_RADIUS;

/** Accent swatches offered as one-click presets, chosen to read well against both appearances. */
export const ACCENT_PRESETS = ["#5b4bd6", "#2f6f4f", "#1d4ed8", "#b91c1c", "#b45309", "#0f766e", "#1f2937"] as const;

export function toFormData(theme: WidgetTheme): WidgetThemeFormData {
    return {
        ...theme,
        suggestedPrompts: theme.suggestedPrompts.map((value) => ({ value })),
    };
}

export function toWidgetTheme(values: WidgetThemeFormOutput): WidgetTheme {
    return {
        ...values,
        headerSubtitle: blankToNull(values.headerSubtitle),
        avatarInitials: blankToNull(values.avatarInitials)?.toUpperCase() ?? null,
        greetingTitle: blankToNull(values.greetingTitle),
        greetingBody: blankToNull(values.greetingBody),
        disclaimer: blankToNull(values.disclaimer),
        suggestedPrompts: values.suggestedPrompts.map((prompt) => prompt.value).filter((value) => value.length > 0),
    };
}

function blankToNull(value: string | null): string | null {
    return value === null || value.length === 0 ? null : value;
}

type PartialFormData = {
    [K in keyof WidgetThemeFormData]?: WidgetThemeFormData[K] extends readonly unknown[]
        ? { value?: string }[] | undefined
        : WidgetThemeFormData[K] | undefined;
};

/** The live preview has to render whatever is in the form right now, including a half-typed hex colour, so
 *  each field falls back to the saved value rather than the whole theme failing to parse. */
export function toPreviewTheme(values: PartialFormData, fallback: WidgetTheme): WidgetTheme {
    // An untouched field is `undefined` and keeps the saved value; a field the operator cleared is `""` or
    // null and must actually clear in the preview.
    const text = (value: string | null | undefined, saved: string | null) =>
        value === undefined ? saved : blankToNull((value ?? "").trim());

    return {
        appearance: values.appearance ?? fallback.appearance,
        accentColor: HEX_COLOR.test((values.accentColor ?? "").trim())
            ? values.accentColor!.trim().toLowerCase()
            : fallback.accentColor,
        radius:
            typeof values.radius === "number" && values.radius >= 0 && values.radius <= MAX_RADIUS
                ? values.radius
                : fallback.radius,
        fontFamily: FONT_STACK.test((values.fontFamily ?? "").trim()) ? values.fontFamily!.trim() : fallback.fontFamily,
        density: values.density ?? fallback.density,
        headerTitle: values.headerTitle?.trim() || fallback.headerTitle,
        headerSubtitle: text(values.headerSubtitle, fallback.headerSubtitle),
        avatarInitials: text(values.avatarInitials, fallback.avatarInitials)?.toUpperCase() ?? null,
        showHeader: values.showHeader ?? fallback.showHeader,
        greetingTitle: text(values.greetingTitle, fallback.greetingTitle),
        greetingBody: text(values.greetingBody, fallback.greetingBody),
        suggestedPrompts: (values.suggestedPrompts ?? [])
            .map((prompt) => prompt?.value?.trim() ?? "")
            .filter((value) => value.length > 0)
            .slice(0, MAX_SUGGESTED_PROMPTS),
        inputPlaceholder: values.inputPlaceholder?.trim() || fallback.inputPlaceholder,
        disclaimer: text(values.disclaimer, fallback.disclaimer),
    };
}
