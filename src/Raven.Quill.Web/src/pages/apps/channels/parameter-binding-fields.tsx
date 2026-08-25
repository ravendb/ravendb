import type { ReactNode } from "react";
import type { Control, FieldValues } from "react-hook-form";
import { cn } from "@/lib/utils";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import type { ParameterSource } from "@/pages/apps/channels/parameter-bindings";

export function ParameterBindingRow<TFieldValues extends FieldValues>({
    control,
    index,
    label,
    source,
    sources,
    sourceHint,
    disabled,
    className,
}: {
    control: Control<TFieldValues>;
    index: number;
    label: string;
    source: ParameterSource | undefined;
    sources: readonly FormSelectOption<ParameterSource>[];
    sourceHint: (source: ParameterSource | undefined) => string | undefined;
    disabled?: boolean;
    className?: string;
}) {
    const rowControl = control as unknown as Control<FieldValues>;
    const hint = sourceHint(source);

    return (
        <div className={cn("grid gap-2", className)}>
            <div className="grid gap-2 sm:grid-cols-2">
                <FormSelect
                    control={rowControl}
                    name={`parameters.${index}.source`}
                    label={label}
                    options={sources}
                    disabled={disabled}
                />
                {source === "Constant" && (
                    <FormInput
                        control={rowControl}
                        name={`parameters.${index}.value`}
                        label="Value"
                        placeholder="e.g. customers/1"
                        disabled={disabled}
                    />
                )}
            </div>
            {hint && <FieldDescription>{hint}</FieldDescription>}
        </div>
    );
}

export function ParameterBindingFields<TFieldValues extends FieldValues>({
    control,
    fields,
    rows,
    sources,
    sourceHint,
    description,
}: {
    control: Control<TFieldValues>;
    fields: readonly { id: string; name: string }[];
    rows: readonly { source?: ParameterSource }[];
    sources: readonly FormSelectOption<ParameterSource>[];
    sourceHint: (source: ParameterSource | undefined) => string | undefined;
    description: ReactNode;
}) {
    if (fields.length === 0) {
        return null;
    }

    return (
        <div className="flex flex-col gap-3">
            <div className="space-y-0.5">
                <h3 className="text-sm font-medium">Parameters</h3>
                <p className="text-xs text-muted-foreground">{description}</p>
            </div>
            {fields.map((field, index) => (
                <ParameterBindingRow
                    key={field.id}
                    control={control}
                    index={index}
                    label={field.name}
                    source={rows[index]?.source}
                    sources={sources}
                    sourceHint={sourceHint}
                />
            ))}
        </div>
    );
}
