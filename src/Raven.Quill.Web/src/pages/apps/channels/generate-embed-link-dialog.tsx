import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch, type Control } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { api } from "@/api/api";
import { cn } from "@/lib/utils";
import type { AgentParameterSummary, MintEmbedLinkRequest, MintEmbedLinkResponse } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { GuardedDialog } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import {
    defaultFormValue,
    elementTypeOf,
    isArrayType,
    PARAMETER_VALUE_TYPES,
    placeholderFor,
    scalarError,
    toJsonValue,
    typeLabelFor,
} from "@/pages/apps/channels/agent-parameter-values";
import { EmbedLinkPreview } from "@/pages/apps/channels/embed-link-preview";
import {
    DEFAULT_MAX_INVOCATIONS,
    MAX_INVOCATIONS,
    MAX_TTL_SECONDS,
    MIN_INVOCATIONS,
    MIN_TTL_SECONDS,
} from "@/pages/apps/channels/embed-link-utils";

type GenerateEmbedLinkDialogProps = {
    slug: string;
    channelId: string;
    displayName: string;
    parameters: AgentParameterSummary[];
    trigger: ReactNode;
};

const DEFAULT_TTL_PRESET = "3600";

const ttlPresetSchema = z.enum(["3600", "14400", "86400", "604800", "custom"]);

const TTL_PRESET_OPTIONS: readonly FormSelectOption<z.infer<typeof ttlPresetSchema>>[] = [
    { value: "3600", label: "1 hour" },
    { value: "14400", label: "4 hours" },
    { value: "86400", label: "24 hours" },
    { value: "604800", label: "7 days" },
    { value: "custom", label: "Custom" },
];

const generateEmbedLinkSchema = z
    .object({
        parameters: z.array(
            z.object({
                name: z.string(),
                type: z.enum(PARAMETER_VALUE_TYPES),
                text: z.string(),
                flag: z.boolean(),
                items: z.array(z.object({ value: z.string() })),
            }),
        ),
        ttlPreset: ttlPresetSchema,
        customTtlSeconds: z.number().int().nullable(),
        maxInvocations: z
            .number({ message: "Enter a number" })
            .int()
            .min(MIN_INVOCATIONS, `Minimum is ${MIN_INVOCATIONS}`)
            .max(MAX_INVOCATIONS, `Maximum is ${MAX_INVOCATIONS.toLocaleString()}`),
    })
    .superRefine((values, ctx) => {
        values.parameters.forEach((parameter, index) => {
            const type = parameter.type;

            if (type === "Null" || type === "Boolean") {
                return;
            }

            if (isArrayType(type)) {
                if (parameter.items.length === 0) {
                    ctx.addIssue({
                        code: "custom",
                        path: ["parameters", index, "items"],
                        message: "Add at least one value",
                    });
                }
                parameter.items.forEach((item, itemIndex) => {
                    const message = scalarError(elementTypeOf(type), item.value);
                    if (message) {
                        ctx.addIssue({
                            code: "custom",
                            path: ["parameters", index, "items", itemIndex, "value"],
                            message,
                        });
                    }
                });
                return;
            }

            const message = scalarError(type, parameter.text);
            if (message) {
                ctx.addIssue({ code: "custom", path: ["parameters", index, "text"], message });
            }
        });

        if (values.ttlPreset !== "custom") {
            return;
        }
        if (values.customTtlSeconds == null) {
            ctx.addIssue({
                code: "custom",
                path: ["customTtlSeconds"],
                message: "Enter a duration in seconds",
            });
        } else if (values.customTtlSeconds < MIN_TTL_SECONDS || values.customTtlSeconds > MAX_TTL_SECONDS) {
            ctx.addIssue({
                code: "custom",
                path: ["customTtlSeconds"],
                message: `Enter ${MIN_TTL_SECONDS}–${MAX_TTL_SECONDS.toLocaleString()} seconds`,
            });
        }
    });

type GenerateEmbedLinkFormData = z.infer<typeof generateEmbedLinkSchema>;

export function GenerateEmbedLinkDialog({
    slug,
    channelId,
    displayName,
    parameters,
    trigger,
}: GenerateEmbedLinkDialogProps) {
    const [isOpen, setIsOpen] = useState(false);

    const getDefaultValues = (): GenerateEmbedLinkFormData => ({
        parameters: parameters.map(defaultFormValue),
        ttlPreset: DEFAULT_TTL_PRESET,
        customTtlSeconds: null,
        maxInvocations: DEFAULT_MAX_INVOCATIONS,
    });

    const form = useForm<GenerateEmbedLinkFormData>({
        resolver: zodResolver(generateEmbedLinkSchema),
        defaultValues: getDefaultValues(),
    });

    const ttlPreset = useWatch({ control: form.control, name: "ttlPreset" });
    const queryClient = useQueryClient();

    const unsavedChanges = useFormUnsavedChanges(form);

    const mintMutation = useMutation({
        mutationFn: (request: MintEmbedLinkRequest) => api.services.embedLinks.mint(slug, request),
        onSuccess: async () => {
            // The minted link replaces the form, so its inputs are spent rather than unsaved.
            unsavedChanges.markSaved();
            await queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey });
        },
    });

    const result = mintMutation.data;

    const submit = form.handleSubmit((values) => {
        const ttlSeconds =
            values.ttlPreset === "custom" && values.customTtlSeconds != null
                ? values.customTtlSeconds
                : Number(values.ttlPreset);
        const bound = Object.fromEntries(
            values.parameters.map((parameter) => [parameter.name, toJsonValue(parameter)]),
        );

        mintMutation.mutate({
            channelId,
            parameters: values.parameters.length > 0 ? bound : undefined,
            ttlSeconds,
            maxInvocations: values.maxInvocations,
        });
    });

    const handleOpenChange = (open: boolean) => {
        setIsOpen(open);
        if (!open) {
            // The minted link's preview iframe spends real invocations (see embed-link-preview.tsx),
            // so refresh the list on close to reflect any chats sent in the preview after minting.
            if (mintMutation.data) {
                queryClient.invalidateQueries({ queryKey: api.queries.embedLinks.list(slug).queryKey });
                queryClient.invalidateQueries({ queryKey: ["stats", "usage"] });
            }
            // Explicit defaults: markSaved made the minted values the baseline, so a bare reset() would restore them.
            form.reset(getDefaultValues());
            mintMutation.reset();
        }
    };

    return (
        // The form lives beside the overlay so "Generate another" can reuse its values - hence the
        // explicit hasUnsavedChanges and the reset on close.
        <GuardedDialog
            open={isOpen}
            onOpenChange={handleOpenChange}
            hasUnsavedChanges={unsavedChanges.hasUnsavedChanges}
        >
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className={cn("sm:max-w-md", result && "sm:max-w-lg")}>
                <DialogHeader>
                    <DialogTitle>Generate embed link</DialogTitle>
                    <DialogDescription>
                        Mint a per-user link for “{displayName}”. Parameters are bound into the link and can’t be
                        changed by the end user.
                    </DialogDescription>
                </DialogHeader>

                {result ? (
                    <MintedLink result={result} onGenerateAnother={() => mintMutation.reset()} />
                ) : (
                    <form className="grid gap-4" onSubmit={submit}>
                        {parameters.length > 0 && (
                            <div className="grid gap-3">
                                {parameters.map((parameter, index) => (
                                    <ParameterField
                                        key={parameter.name}
                                        control={form.control}
                                        parameter={parameter}
                                        index={index}
                                    />
                                ))}
                            </div>
                        )}

                        <FormSelect
                            control={form.control}
                            name="ttlPreset"
                            label="Link expires after"
                            options={TTL_PRESET_OPTIONS}
                        />
                        {ttlPreset === "custom" && (
                            <FormInput
                                control={form.control}
                                name="customTtlSeconds"
                                type="number"
                                label="Custom duration (seconds)"
                                placeholder="e.g. 7200"
                                min={MIN_TTL_SECONDS}
                                max={MAX_TTL_SECONDS}
                            />
                        )}

                        <FormInput
                            control={form.control}
                            name="maxInvocations"
                            type="number"
                            label="Max invocations"
                            description="How many chats this link allows before it stops working."
                            min={MIN_INVOCATIONS}
                            max={MAX_INVOCATIONS}
                        />

                        {mintMutation.isError && (
                            <Alert variant="destructive">
                                {mintMutation.error instanceof Error
                                    ? mintMutation.error.message
                                    : "Could not generate link."}
                            </Alert>
                        )}

                        <DialogFooter>
                            <DialogClose asChild>
                                <Button type="button" variant="outline">
                                    Cancel
                                </Button>
                            </DialogClose>
                            <Button type="submit" disabled={mintMutation.isPending}>
                                {mintMutation.isPending && <Spinner />}
                                Generate link
                            </Button>
                        </DialogFooter>
                    </form>
                )}
            </DialogContent>
        </GuardedDialog>
    );
}

function ParameterField({
    control,
    parameter,
    index,
}: {
    control: Control<GenerateEmbedLinkFormData>;
    parameter: AgentParameterSummary;
    index: number;
}) {
    const typeLabel = typeLabelFor(parameter.type);
    const description = [parameter.description, typeLabel].filter(Boolean).join(" · ") || undefined;

    if (parameter.type === "Null") {
        return (
            <Field>
                <FieldLabel>{parameter.name}</FieldLabel>
                <FieldDescription>Always bound as null.</FieldDescription>
            </Field>
        );
    }

    if (parameter.type === "Boolean") {
        return (
            <Field>
                <FormSwitch control={control} name={`parameters.${index}.flag`} label={parameter.name} />
                {description && <FieldDescription>{description}</FieldDescription>}
            </Field>
        );
    }

    if (isArrayType(parameter.type)) {
        return (
            <FormStringList
                control={control}
                name={`parameters.${index}.items`}
                label={parameter.name}
                description={description}
                addButtonLabel="Add value"
                emptyLabel="No values."
                defaultValue={{ value: "" }}
                fieldName={(itemIndex) => `parameters.${index}.items.${itemIndex}.value`}
                itemLabel={(itemIndex) => `Value ${itemIndex + 1}`}
                placeholder={placeholderFor(parameter.type)}
            />
        );
    }

    return (
        <FormInput
            control={control}
            name={`parameters.${index}.text`}
            label={parameter.name}
            description={description}
            placeholder={placeholderFor(parameter.type)}
        />
    );
}

function MintedLink({ result, onGenerateAnother }: { result: MintEmbedLinkResponse; onGenerateAnother: () => void }) {
    return (
        <>
            <EmbedLinkPreview url={result.url} expiresAt={result.expiresAt} maxInvocations={result.maxInvocations} />
            <DialogFooter>
                <Button type="button" variant="secondary" onClick={onGenerateAnother}>
                    Generate another
                </Button>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Done
                    </Button>
                </DialogClose>
            </DialogFooter>
        </>
    );
}
