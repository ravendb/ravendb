import { z } from "zod";
import { findCssSyntaxError } from "@/pages/apps/channels/custom-css-syntax";
import type {
    WidgetFontSize,
    WidgetLogoRadius,
    WidgetRadius,
    WidgetTheme,
    WidgetThemeColors,
} from "@/api/generated/server-api";

// Mirrors WidgetThemeValidation.cs. The server is still the authority; these bounds exist so the operator
// gets the message next to the field instead of after a round trip. Every one is named rather than inlined
// so a change on the server has a single place to land here.
export const MAX_SUGGESTED_PROMPTS = 10;
const MAX_SUGGESTED_PROMPT_LENGTH = 200;
export const MAX_HEADER_TITLE_LENGTH = 120;
const MAX_HEADER_SUBTITLE_LENGTH = 200;
const MAX_GREETING_TITLE_LENGTH = 160;
const MAX_GREETING_BODY_LENGTH = 1_000;
const MAX_INPUT_PLACEHOLDER_LENGTH = 160;
const MAX_DISCLAIMER_LENGTH = 600;
const MAX_FONT_FAMILY_LENGTH = 200;
const MAX_CUSTOM_CSS_LENGTH = 10_000;
const MAX_LOGO_LENGTH = 150_000;

export const MIN_CUSTOM_FONT_SIZE_REM = 0.625;
export const MAX_CUSTOM_FONT_SIZE_REM = 1.5;

const HEX_COLOR = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

// Everything a font stack legitimately needs; rules out `;`, `{`, `}`, `@` and `url(` by construction.
const FONT_STACK = /^[A-Za-z0-9 ,'"-]+$/;

// The custom CSS ships inside the embed shell's <style> tag, which this sequence would close.
const STYLE_CLOSE = /<\/style/i;

// Raster formats only: an SVG data URI can carry script.
const LOGO_DATA_URI = /^data:image\/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+$/;

const RADIUS_VALUES = ["None", "Small", "Medium", "Large"] as const;
const LOGO_RADIUS_VALUES = ["None", "Small", "Medium", "Large", "Pill"] as const;
const FONT_SIZE_VALUES = ["Small", "Medium", "Large", "Custom"] as const;

const optionalText = (max: number, label: string) =>
    z.string().trim().max(max, `${label} must be ${max} characters or fewer`).nullable();

const hexColor = z
    .string()
    .trim()
    .regex(HEX_COLOR, "Enter a hex color such as #2f6f4f")
    .transform((value) => value.toLowerCase());

export const widgetThemeSchema = z
    .object({
        appearance: z.enum(["Light", "Dark", "System"]),
        lightButtonColor: hexColor,
        lightMessageColor: hexColor,
        lightBackgroundColor: hexColor,
        darkButtonColor: hexColor,
        darkMessageColor: hexColor,
        darkBackgroundColor: hexColor,
        radius: z.enum(RADIUS_VALUES),
        fontFamily: z
            .string()
            .trim()
            .min(1, "Pick a font")
            .max(MAX_FONT_FAMILY_LENGTH, `Font stack must be ${MAX_FONT_FAMILY_LENGTH} characters or fewer`)
            .regex(FONT_STACK, "A font stack may only contain letters, digits, spaces, commas, hyphens and quotes"),
        fontSize: z.enum(FONT_SIZE_VALUES),
        // Bounds live in the superRefine below, together with every other rule that only applies while the
        // option it belongs to is switched on.
        customFontSizeRem: z.number("Enter a number").nullable(),
        logo: z
            .string()
            .max(MAX_LOGO_LENGTH, "That image is too large even after downscaling")
            .refine((value) => value.length === 0 || LOGO_DATA_URI.test(value), "Upload a png, jpeg or webp image"),
        logoRadius: z.enum(LOGO_RADIUS_VALUES),
        headerTitle: z
            .string()
            .trim()
            .max(MAX_HEADER_TITLE_LENGTH, `Title must be ${MAX_HEADER_TITLE_LENGTH} characters or fewer`),
        headerSubtitle: optionalText(MAX_HEADER_SUBTITLE_LENGTH, "Subtitle"),
        showHeader: z.boolean(),
        greetingTitle: optionalText(MAX_GREETING_TITLE_LENGTH, "Greeting title"),
        greetingBody: optionalText(MAX_GREETING_BODY_LENGTH, "Greeting body"),
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
            // A blank row is dropped on save, so only the prompts that will actually ship count - matching
            // the server, which normalizes the list before it counts.
            .refine(
                (prompts) => prompts.filter((prompt) => prompt.value.length > 0).length <= MAX_SUGGESTED_PROMPTS,
                `Up to ${MAX_SUGGESTED_PROMPTS} prompts`,
            ),
        inputPlaceholder: z
            .string()
            .trim()
            .min(1, "A placeholder is required")
            .max(
                MAX_INPUT_PLACEHOLDER_LENGTH,
                `Placeholder must be ${MAX_INPUT_PLACEHOLDER_LENGTH} characters or fewer`,
            ),
        disclaimer: optionalText(MAX_DISCLAIMER_LENGTH, "Disclaimer"),
        customCss: z
            .string()
            .max(
                MAX_CUSTOM_CSS_LENGTH,
                `Custom CSS must be ${MAX_CUSTOM_CSS_LENGTH.toLocaleString()} characters or fewer`,
            )
            .refine((value) => STYLE_CLOSE.test(value) === false, 'Custom CSS may not contain "</style"')
            .superRefine((value, ctx) => {
                const syntaxError = findCssSyntaxError(value);

                if (syntaxError) {
                    ctx.addIssue({ code: "custom", message: syntaxError });
                }
            })
            .transform((value) => value.trim()),
    })
    // A rule that belongs to a switched-off option must not block saving: the value behind it is either
    // invisible or unused, so the operator would be stuck on an error they cannot see the cause of.
    .superRefine((data, ctx) => {
        if (data.showHeader && data.headerTitle.length === 0) {
            ctx.addIssue({ code: "custom", path: ["headerTitle"], message: "A title is required" });
        }

        if (data.fontSize !== "Custom") return;

        if (data.customFontSizeRem === null) {
            ctx.addIssue({ code: "custom", path: ["customFontSizeRem"], message: "Enter a size in rem, e.g. 1.05" });
        } else if (data.customFontSizeRem < MIN_CUSTOM_FONT_SIZE_REM) {
            ctx.addIssue({
                code: "custom",
                path: ["customFontSizeRem"],
                message: `At least ${MIN_CUSTOM_FONT_SIZE_REM} rem`,
            });
        } else if (data.customFontSizeRem > MAX_CUSTOM_FONT_SIZE_REM) {
            ctx.addIssue({
                code: "custom",
                path: ["customFontSizeRem"],
                message: `At most ${MAX_CUSTOM_FONT_SIZE_REM} rem`,
            });
        }
    });

/** The form's shape. `suggestedPrompts` is a list of objects because `useFieldArray` needs stable keys. */
export type WidgetThemeFormData = z.input<typeof widgetThemeSchema>;

/** What the resolver hands to `handleSubmit`: trimmed and normalized, but still the form's prompt shape. */
export type WidgetThemeFormOutput = z.output<typeof widgetThemeSchema>;

export const RADIUS_OPTIONS = RADIUS_VALUES.map((value) => ({ value, label: value }));
export const LOGO_RADIUS_OPTIONS = LOGO_RADIUS_VALUES.map((value) => ({ value, label: value }));
export const FONT_SIZE_OPTIONS = FONT_SIZE_VALUES.map((value) => ({ value, label: value }));

export function toFormData(theme: WidgetTheme): WidgetThemeFormData {
    return {
        appearance: theme.appearance,
        lightButtonColor: theme.light.buttonColor,
        lightMessageColor: theme.light.messageColor,
        lightBackgroundColor: theme.light.backgroundColor,
        darkButtonColor: theme.dark.buttonColor,
        darkMessageColor: theme.dark.messageColor,
        darkBackgroundColor: theme.dark.backgroundColor,
        radius: theme.radius,
        fontFamily: theme.fontFamily,
        fontSize: theme.fontSize,
        customFontSizeRem: theme.customFontSizeRem,
        logo: theme.logo ?? "",
        logoRadius: theme.logoRadius,
        headerTitle: theme.headerTitle,
        headerSubtitle: theme.headerSubtitle,
        showHeader: theme.showHeader,
        greetingTitle: theme.greetingTitle,
        greetingBody: theme.greetingBody,
        suggestedPrompts: theme.suggestedPrompts.map((value) => ({ value })),
        inputPlaceholder: theme.inputPlaceholder,
        disclaimer: theme.disclaimer,
        customCss: theme.customCss ?? "",
    };
}

export function toWidgetTheme(values: WidgetThemeFormOutput): WidgetTheme {
    return {
        appearance: values.appearance,
        light: {
            buttonColor: values.lightButtonColor,
            messageColor: values.lightMessageColor,
            backgroundColor: values.lightBackgroundColor,
        },
        dark: {
            buttonColor: values.darkButtonColor,
            messageColor: values.darkMessageColor,
            backgroundColor: values.darkBackgroundColor,
        },
        radius: values.radius,
        fontFamily: values.fontFamily,
        fontSize: values.fontSize,
        customFontSizeRem: values.fontSize === "Custom" ? values.customFontSizeRem : null,
        logo: blankToNull(values.logo),
        logoRadius: values.logoRadius,
        headerTitle: values.headerTitle,
        headerSubtitle: blankToNull(values.headerSubtitle),
        showHeader: values.showHeader,
        greetingTitle: blankToNull(values.greetingTitle),
        greetingBody: blankToNull(values.greetingBody),
        suggestedPrompts: values.suggestedPrompts.map((prompt) => prompt.value).filter((value) => value.length > 0),
        inputPlaceholder: values.inputPlaceholder,
        disclaimer: blankToNull(values.disclaimer),
        customCss: blankToNull(values.customCss),
    };
}

function blankToNull(value: string | null): string | null {
    return value === null || value.length === 0 ? null : value;
}

function isRadius(value: unknown): value is WidgetRadius {
    return RADIUS_VALUES.includes(value as WidgetRadius);
}

function isLogoRadius(value: unknown): value is WidgetLogoRadius {
    return LOGO_RADIUS_VALUES.includes(value as WidgetLogoRadius);
}

function isFontSize(value: unknown): value is WidgetFontSize {
    return FONT_SIZE_VALUES.includes(value as WidgetFontSize);
}

type PartialFormData = {
    [K in keyof WidgetThemeFormData]?: WidgetThemeFormData[K] extends readonly unknown[]
        ? { value?: string }[] | undefined
        : WidgetThemeFormData[K] | undefined;
};

/** The live preview has to render whatever is in the form right now, including a half-typed hex color, so
 *  each field falls back to the saved value rather than the whole theme failing to parse. */
export function toPreviewTheme(values: PartialFormData, fallback: WidgetTheme): WidgetTheme {
    // An untouched field is `undefined` and keeps the saved value; a field the operator cleared is `""` or
    // null and must actually clear in the preview.
    const text = (value: string | null | undefined, saved: string | null) =>
        value === undefined ? saved : blankToNull((value ?? "").trim());

    // A half-typed hex keeps the saved color rather than flashing a fallback.
    const color = (value: string | undefined, saved: string) => {
        const trimmed = (value ?? "").trim();
        return HEX_COLOR.test(trimmed) ? trimmed.toLowerCase() : saved;
    };

    const colors = (
        buttonValue: string | undefined,
        messageValue: string | undefined,
        backgroundValue: string | undefined,
        saved: WidgetThemeColors,
    ): WidgetThemeColors => ({
        buttonColor: color(buttonValue, saved.buttonColor),
        messageColor: color(messageValue, saved.messageColor),
        backgroundColor: color(backgroundValue, saved.backgroundColor),
    });

    const fontSize = isFontSize(values.fontSize) ? values.fontSize : fallback.fontSize;
    const customFontSizeRem =
        typeof values.customFontSizeRem === "number" &&
        values.customFontSizeRem >= MIN_CUSTOM_FONT_SIZE_REM &&
        values.customFontSizeRem <= MAX_CUSTOM_FONT_SIZE_REM
            ? values.customFontSizeRem
            : fallback.customFontSizeRem;

    return {
        appearance: values.appearance ?? fallback.appearance,
        light: colors(values.lightButtonColor, values.lightMessageColor, values.lightBackgroundColor, fallback.light),
        dark: colors(values.darkButtonColor, values.darkMessageColor, values.darkBackgroundColor, fallback.dark),
        radius: isRadius(values.radius) ? values.radius : fallback.radius,
        fontFamily: FONT_STACK.test((values.fontFamily ?? "").trim()) ? values.fontFamily!.trim() : fallback.fontFamily,
        fontSize,
        customFontSizeRem: fontSize === "Custom" ? customFontSizeRem : null,
        logo: values.logo === undefined ? fallback.logo : blankToNull(values.logo),
        logoRadius: isLogoRadius(values.logoRadius) ? values.logoRadius : fallback.logoRadius,
        headerTitle: values.headerTitle?.trim() || fallback.headerTitle,
        headerSubtitle: text(values.headerSubtitle, fallback.headerSubtitle),
        showHeader: values.showHeader ?? fallback.showHeader,
        greetingTitle: text(values.greetingTitle, fallback.greetingTitle),
        greetingBody: text(values.greetingBody, fallback.greetingBody),
        suggestedPrompts: (values.suggestedPrompts ?? [])
            .map((prompt) => prompt?.value?.trim() ?? "")
            .filter((value) => value.length > 0)
            .slice(0, MAX_SUGGESTED_PROMPTS),
        inputPlaceholder: values.inputPlaceholder?.trim() || fallback.inputPlaceholder,
        disclaimer: text(values.disclaimer, fallback.disclaimer),
        customCss: values.customCss === undefined ? fallback.customCss : blankToNull(values.customCss.trim()),
    };
}
