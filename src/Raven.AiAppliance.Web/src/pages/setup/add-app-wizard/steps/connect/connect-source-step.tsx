import { useController, useFormContext } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { PROVIDER_OPTIONS } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";

export function ConnectSourceStep({ isBusy }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const {
        field: { value, onChange },
        fieldState: { error, invalid },
    } = useController({ control, name: "externalConnection.provider" });

    return (
        <div className="grid gap-5">
            <FormInput
                control={control}
                name="externalConnection.appName"
                label="Application name"
                placeholder="e.g. AcmeShop"
                disabled={isBusy}
            />
            <Field data-invalid={invalid}>
                <FieldLabel>Source database type</FieldLabel>
                <div className="grid gap-3 sm:grid-cols-3">
                    {PROVIDER_OPTIONS.map((option) => {
                        const isSelected = option.value === value;

                        return (
                            <button
                                key={option.value}
                                type="button"
                                aria-pressed={isSelected}
                                onClick={() => onChange(option.value)}
                                disabled={isBusy}
                                className={cn(
                                    "flex min-h-28 flex-col items-center justify-center gap-3 rounded-lg border bg-background p-4 transition-colors",
                                    "hover:bg-accent hover:text-accent-foreground",
                                    isSelected && "border-foreground bg-accent text-accent-foreground",
                                    isBusy && "cursor-not-allowed opacity-55 hover:bg-background hover:text-foreground",
                                )}
                            >
                                {option.icon}
                                <span className="text-sm font-semibold">{option.label}</span>
                            </button>
                        );
                    })}
                </div>
                {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            </Field>
            <FormTextarea
                control={control}
                name="externalConnection.connectionString"
                label="Connection string"
                placeholder="Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass"
                textareaClassName="font-mono text-xs"
                disabled={isBusy}
            />
        </div>
    );
}
