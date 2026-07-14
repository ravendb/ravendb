// The widget's themable :root variables (see IFrameStyleVariables server-side). The kind picks
// the input: color swatch, px number, or free text for the font stack.
export const THEME_VARIABLES = [
    { name: "--ai-bg", label: "Background", kind: "color" },
    { name: "--ai-fg", label: "Text", kind: "color" },
    { name: "--ai-border-color", label: "Border", kind: "color" },
    { name: "--ai-bubble-agent-bg", label: "Agent bubble", kind: "color" },
    { name: "--ai-user-bg", label: "User bubble", kind: "color" },
    { name: "--ai-user-fg", label: "User bubble text", kind: "color" },
    { name: "--ai-input-bg", label: "Input background", kind: "color" },
    { name: "--ai-input-border-color", label: "Input border", kind: "color" },
    { name: "--ai-radius-bubble", label: "Bubble radius (px)", kind: "px" },
    { name: "--ai-radius-control", label: "Control radius (px)", kind: "px" },
    { name: "--ai-font-family", label: "Font family", kind: "font" },
] as const;

export type ThemeVariable = (typeof THEME_VARIABLES)[number];

export type ThemeVariableName = ThemeVariable["name"];

export type ThemeVariableValues = Partial<Record<ThemeVariableName, string>>;

const THEME_VARIABLE_NAMES = new Set<string>(THEME_VARIABLES.map((variable) => variable.name));

function isThemeVariableName(name: string): name is ThemeVariableName {
    return THEME_VARIABLE_NAMES.has(name);
}

/** Reads the theme variables declared anywhere in the CSS; a later declaration wins, matching the cascade. */
export function parseThemeVariables(css: string): ThemeVariableValues {
    const values: ThemeVariableValues = {};
    for (const [, name, value] of css.matchAll(/(--ai-[\w-]+)\s*:\s*([^;}]+)/g)) {
        if (isThemeVariableName(name)) values[name] = value.trim();
    }
    return values;
}

export function buildThemeCss(values: ThemeVariableValues): string {
    const declarations = THEME_VARIABLES.flatMap(({ name }) => {
        const value = values[name]?.trim();
        return value ? [`    ${name}: ${value};`] : [];
    });
    return `:root {\n${declarations.join("\n")}\n}`;
}

/** The overrides that effectively change the defaults; empty, unchanged, and half-typed
 *  values (e.g. a partial hex color still being edited) are dropped. */
export function changedThemeVariables(
    defaults: ThemeVariableValues,
    overrides: ThemeVariableValues,
): ThemeVariableValues {
    const changed: ThemeVariableValues = {};
    for (const { name, kind } of THEME_VARIABLES) {
        const value = overrides[name]?.trim();
        if (!value || value === defaults[name]) continue;
        if (kind === "color" && !CSS.supports("color", value)) continue;
        changed[name] = value;
    }
    return changed;
}
