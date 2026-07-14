import { useId } from "react";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { Input } from "@/components/shadcn/ui/input";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";
import {
    THEME_VARIABLES,
    type ThemeVariable,
    type ThemeVariableName,
    type ThemeVariableValues,
} from "@/pages/apps/channels/web-widget-theme-css";

// <input type="color"> only accepts #rrggbb, but a custom app default may declare any CSS color.
function toColorInputValue(value: string): string {
    const color = value.trim().toLowerCase();
    if (/^#[0-9a-f]{6}$/.test(color)) return color;
    if (/^#[0-9a-f]{3}$/.test(color)) return `#${[...color.slice(1)].map((digit) => digit + digit).join("")}`;
    return "#000000";
}

function toPxInputValue(value: string): string {
    const pixels = Number.parseFloat(value);
    return Number.isFinite(pixels) ? String(pixels) : "";
}

type ThemeVariableFieldProps = {
    variable: ThemeVariable;
    value: string;
    disabled?: boolean;
    onChange: (value: string) => void;
};

function ThemeVariableField({ variable, value, disabled, onChange }: ThemeVariableFieldProps) {
    const id = useId();

    return (
        <Field className={variable.kind === "font" ? "sm:col-span-2" : undefined}>
            <FieldLabel htmlFor={id} title={variable.name}>
                {variable.label}
            </FieldLabel>
            {variable.kind === "color" && (
                <InputGroup>
                    <InputGroupAddon>
                        <input
                            type="color"
                            aria-label={`${variable.label} color picker`}
                            className="size-5 shrink-0 cursor-pointer appearance-none rounded-full border-none bg-transparent p-0 ring-1 ring-foreground/20 ring-inset disabled:cursor-not-allowed"
                            value={toColorInputValue(value)}
                            disabled={disabled}
                            onChange={(event) => onChange(event.target.value)}
                        />
                    </InputGroupAddon>
                    <InputGroupInput
                        id={id}
                        type="text"
                        value={value}
                        disabled={disabled}
                        onChange={(event) => onChange(event.target.value)}
                    />
                </InputGroup>
            )}
            {variable.kind === "px" && (
                <Input
                    id={id}
                    type="number"
                    min={0}
                    value={toPxInputValue(value)}
                    disabled={disabled}
                    onChange={(event) =>
                        onChange(event.target.value === "" ? "" : `${Math.max(0, Number(event.target.value))}px`)
                    }
                />
            )}
            {variable.kind === "font" && (
                <Input
                    id={id}
                    type="text"
                    value={value}
                    disabled={disabled}
                    onChange={(event) => onChange(event.target.value)}
                />
            )}
        </Field>
    );
}

type WebWidgetThemeVariablesProps = {
    /** The effective value per variable (the selected style's value with any pending edits applied). */
    values: ThemeVariableValues;
    disabled?: boolean;
    onValueChange: (name: ThemeVariableName, value: string) => void;
};

export function WebWidgetThemeVariables({ values, disabled, onValueChange }: WebWidgetThemeVariablesProps) {
    return (
        <div className="grid content-start gap-3 rounded-lg border p-4 sm:grid-cols-2">
            {THEME_VARIABLES.map((variable) => (
                <ThemeVariableField
                    key={variable.name}
                    variable={variable}
                    value={values[variable.name] ?? ""}
                    disabled={disabled}
                    onChange={(value) => onValueChange(variable.name, value)}
                />
            ))}
            <p className="text-xs text-muted-foreground sm:col-span-2">
                Saving with changes here stores the result as custom CSS.
            </p>
        </div>
    );
}
