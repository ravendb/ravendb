import { useController, useFormContext, useFormState } from "react-hook-form";
import { CARD_LABEL_CLASSES, SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FormInput } from "@/components/form/form-input";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { PROVIDER_OPTIONS } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";
import { ConnectionEditor } from "@/pages/setup/add-app-wizard/steps/connect/connection-editor";
import { TestConnectionButton } from "@/pages/setup/add-app-wizard/steps/connect/test-connection-button";
import { toSlug } from "@/pages/setup/add-app-wizard/slugify";
import { InputGroupAddon } from "@/components/shadcn/ui/input-group";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { RefreshCw } from "lucide-react";

export function ConnectSourceStep({ isBusy }: WizardBodyComponentProps) {
    const { control, setValue, getValues } = useFormContext<AppFormData>();
    const { touchedFields } = useFormState({ control });
    const isEditingApp = useSetupWizardStore((state) => state.editedAppSlug !== null);

    // The slug follows the app name until the operator touches it, and never on an existing app,
    // where it is already the app's database name.
    const isSlugFollowingName = !touchedFields.externalConnection?.slug && !isEditingApp;

    const {
        field: { value },
        fieldState: { error, invalid },
    } = useController({ control, name: "externalConnection.provider" });

    const isProviderSelected = Boolean(value);

    return (
        <div className="grid gap-5">
            <FormInput
                control={control}
                name="externalConnection.appName"
                label="App name"
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
                label="Public URL slug"
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
                                disabled={isBusy}
                                className={cn(
                                    "flex items-center gap-3 rounded-lg border bg-background px-4 py-3 transition-colors",
                                    isSelected && SELECTED_CARD_CLASSES,
                                    !isSelected && !isBusy && "hover:bg-accent hover:text-accent-foreground",
                                    isBusy && "cursor-not-allowed opacity-55",
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
            <TooltipProvider>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <div className="grid gap-5">
                            <ConnectionEditor isDisabled={isBusy || !isProviderSelected} />
                            <TestConnectionButton disabled={isBusy || !isProviderSelected} />
                        </div>
                    </TooltipTrigger>
                    {!isProviderSelected && !isBusy && (
                        <TooltipContent>
                            Select a source database type to fill in its connection details.
                        </TooltipContent>
                    )}
                </Tooltip>
            </TooltipProvider>
        </div>
    );
}
