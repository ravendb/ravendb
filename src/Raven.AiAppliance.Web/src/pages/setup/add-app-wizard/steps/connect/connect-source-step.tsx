import { useController, useFormContext } from "react-hook-form";
import { SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FormInput } from "@/components/form/form-input";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { ImportedConfigAlert } from "@/pages/setup/add-app-wizard/imported-config-alert";
import { PROVIDER_OPTIONS } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { ImportConfigDialog } from "@/pages/setup/add-app-wizard/steps/connect/import-config-dialog";

export function ConnectSourceStep({ isBusy }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const importState = useSetupWizardStore((state) => state.importState);
    const isLocked = importState === "locked";

    const {
        field: { value, onChange },
        fieldState: { error, invalid },
    } = useController({ control, name: "externalConnection.provider" });

    const isConnectionDisabled = isBusy || isLocked;

    return (
        <div className="grid gap-5">
            <ImportedConfigAlert />
            {importState === "none" && (
                <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/30 px-4 py-3">
                    <div className="grid gap-0.5">
                        <span className="text-sm font-medium">Have an exported configuration?</span>
                        <span className="text-xs text-muted-foreground">
                            Import it to reuse a saved connection and table mapping.
                        </span>
                    </div>
                    <ImportConfigDialog disabled={isBusy} />
                </div>
            )}
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
                                disabled={isConnectionDisabled}
                                className={cn(
                                    "flex min-h-28 flex-col items-center justify-center gap-3 rounded-lg border bg-background p-4 transition-colors",
                                    isSelected && SELECTED_CARD_CLASSES,
                                    !isSelected &&
                                        !isConnectionDisabled &&
                                        "hover:bg-accent hover:text-accent-foreground",
                                    isConnectionDisabled && "cursor-not-allowed opacity-55",
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
                disabled={isConnectionDisabled}
            />
        </div>
    );
}
