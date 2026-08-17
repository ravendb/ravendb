import { useController, useFormContext, useFormState, useWatch } from "react-hook-form";
import { CARD_LABEL_CLASSES, SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FormInput } from "@/components/form/form-input";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { LockedConfigAlert } from "@/pages/setup/add-app-wizard/locked-config-alert";
import { PROVIDER_OPTIONS } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { ConnectionEditor } from "@/pages/setup/add-app-wizard/steps/connect/connection-editor";
import { ImportConfigDialog } from "@/pages/setup/add-app-wizard/steps/connect/import-config-dialog";
import { TestConnectionButton } from "@/pages/setup/add-app-wizard/steps/connect/test-connection-button";
import { toSlug } from "@/pages/setup/add-app-wizard/slugify";
import { InputGroupAddon } from "@/components/shadcn/ui/input-group";
import { Button } from "@/components/shadcn/ui/button";
import { RefreshCw } from "lucide-react";

export function ConnectSourceStep({ isBusy }: WizardBodyComponentProps) {
    const { control, setValue, getValues } = useFormContext<AppFormData>();
    const { touchedFields } = useFormState({ control });
    const configLock = useSetupWizardStore((state) => state.configLock);
    const isEditingApp = useSetupWizardStore((state) => state.editedAppSlug !== null);
    const isLocked = configLock === "locked";

    // The slug follows the app name until the operator touches it, and never on an existing app,
    // where it is already the app's database name.
    const isSlugFollowingName = !touchedFields.externalConnection?.slug && !isEditingApp;

    const appName = useWatch({ control, name: "externalConnection.appName" });
    const slug = useWatch({ control, name: "externalConnection.slug" });
    // The import runs through the wizard endpoints, which key their state by slug.
    const isImportReady = slug.trim() !== "" || toSlug(appName) !== "";

    const {
        field: { value },
        fieldState: { error, invalid },
    } = useController({ control, name: "externalConnection.provider" });

    // A locked configuration owns its connection until the operator enables editing.
    const isConnectionDisabled = isBusy || isLocked;

    return (
        <div className="grid gap-5">
            <LockedConfigAlert />
            {configLock === "none" && (
                <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/30 px-4 py-3">
                    <div className="grid gap-0.5">
                        <span className="text-sm font-medium">Have an exported configuration?</span>
                        <span className="text-xs text-muted-foreground">
                            Import it to reuse a saved connection and table mapping.
                        </span>
                    </div>
                    <ImportConfigDialog
                        disabled={isBusy || !isImportReady}
                        disabledExplanation={
                            isImportReady ? undefined : "Enter an application name first to import a configuration."
                        }
                    />
                </div>
            )}
            <FormInput
                control={control}
                name="externalConnection.appName"
                label="Application name"
                placeholder="e.g. AcmeShop"
                disabled={isBusy}
                afterChange={(event) => {
                    if (isSlugFollowingName) {
                        setValue("externalConnection.slug", toSlug(event.target.value), { shouldValidate: true });
                    }
                }}
            />
            <FormInput
                control={control}
                name="externalConnection.slug"
                label={isEditingApp ? "Public URL slug" : "Public URL slug (optional)"}
                placeholder="e.g. acme-shop"
                disabled={isBusy || isEditingApp}
                description={
                    isEditingApp
                        ? "Appears in every public embed URL and is the app's database name, so it cannot be changed."
                        : "Appears in every public embed URL and becomes the app's database name. Permanent once the app is created."
                }
                addons={
                    !isEditingApp && (
                        <InputGroupAddon align="inline-end">
                            <Button
                                variant="ghost"
                                onClick={() =>
                                    setValue(
                                        "externalConnection.slug",
                                        toSlug(getValues("externalConnection.appName")),
                                        { shouldValidate: true },
                                    )
                                }
                            >
                                <RefreshCw />
                                Regenerate
                            </Button>
                        </InputGroupAddon>
                    )
                }
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
                                onClick={() =>
                                    setValue("externalConnection.provider", option.value, {
                                        shouldDirty: true,
                                        shouldValidate: true,
                                    })
                                }
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
                                <span className={CARD_LABEL_CLASSES}>{option.label}</span>
                            </button>
                        );
                    })}
                </div>
                {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            </Field>
            <ConnectionEditor isDisabled={isConnectionDisabled} />
            <TestConnectionButton disabled={isConnectionDisabled} />
        </div>
    );
}
