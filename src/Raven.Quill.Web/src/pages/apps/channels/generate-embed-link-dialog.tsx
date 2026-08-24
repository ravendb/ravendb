import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { api } from "@/api/api";
import { cn } from "@/lib/utils";
import { getAgentParameterPlaceholder, getAgentParameterValueError } from "@/lib/agent-parameter-values";
import type {
    AiAgentParameterValueType,
    MintEmbedLinkRequest,
    MintEmbedLinkResponse,
} from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
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
    /** Agent the channel is bound to; its declared parameter types drive the value inputs. */
    agentId: string | undefined;
    displayName: string;
    /** The agent's declared parameter names — one value is bound per name at mint time. */
    parameterNames: string[];
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

// Rebuilt whenever the agent's declared parameter types (re)load; until then unknown
// names skip the typed value check and only the "required" rule applies.
const buildGenerateEmbedLinkSchema = (parameterTypeByName: ReadonlyMap<string, AiAgentParameterValueType>) =>
    z
        .object({
            parameters: z.array(z.object({ name: z.string(), value: z.string().trim().min(1, "Required") })),
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
                const type = parameterTypeByName.get(parameter.name);
                if (!parameter.value || !type) {
                    return;
                }
                const error = getAgentParameterValueError(parameter.value, type);
                if (error) {
                    ctx.addIssue({ code: "custom", path: ["parameters", index, "value"], message: error });
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

type GenerateEmbedLinkFormData = z.infer<ReturnType<typeof buildGenerateEmbedLinkSchema>>;

export function GenerateEmbedLinkDialog({
    slug,
    channelId,
    agentId,
    displayName,
    parameterNames,
    trigger,
}: GenerateEmbedLinkDialogProps) {
    const [isOpen, setIsOpen] = useState(false);

    // The channel list only carries parameter names; the declared types come from the agent
    // details, fetched lazily when the dialog opens. Until they load (or if the fetch fails)
    // the inputs keep a generic placeholder and skip the typed value check.
    const agentDetailsQuery = useQuery({
        ...api.queries.agents.detail(slug, agentId ?? ""),
        enabled: isOpen && !!agentId,
    });
    const parameterTypeByName = new Map<string, AiAgentParameterValueType>(
        (agentDetailsQuery.data?.configuration.parameters ?? [])
            .filter((parameter) => parameter.name)
            .map((parameter) => [parameter.name!, parameter.type ?? "Default"]),
    );

    const getDefaultValues = (): GenerateEmbedLinkFormData => ({
        parameters: parameterNames.map((name) => ({ name, value: "" })),
        ttlPreset: DEFAULT_TTL_PRESET,
        customTtlSeconds: null,
        maxInvocations: DEFAULT_MAX_INVOCATIONS,
    });

    const form = useForm<GenerateEmbedLinkFormData>({
        resolver: zodResolver(buildGenerateEmbedLinkSchema(parameterTypeByName)),
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
        const parameters = Object.fromEntries(values.parameters.map(({ name, value }) => [name, value.trim()]));

        mintMutation.mutate({
            channelId,
            parameters: values.parameters.length > 0 ? parameters : undefined,
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
                        {parameterNames.length > 0 && (
                            <div className="grid gap-3">
                                {parameterNames.map((name, index) => (
                                    <FormInput
                                        key={name}
                                        control={form.control}
                                        name={`parameters.${index}.value`}
                                        label={name}
                                        placeholder={getAgentParameterPlaceholder(
                                            parameterTypeByName.get(name) ?? "Default",
                                        )}
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
