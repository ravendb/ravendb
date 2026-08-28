import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import type { AppResponse } from "@/api/generated/server-api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
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
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import {
    CLEARANCE_OPTIONS,
    DATABASE_ACCESS_OPTIONS,
    toDatabaseOption,
} from "@/pages/dashboard/certificates/certificate-labels";
import {
    permissionRowSchema,
    reportPermissionRowIssues,
    toPermissionsRecord,
} from "@/pages/dashboard/certificates/certificate-permissions";

const generateCertificateSchema = z
    .object({
        name: z.string().trim().min(1, "Required"),
        password: z.string(),
        clearance: z.enum(["Operator", "ValidUser"]),
        permissions: z.array(permissionRowSchema),
    })
    .superRefine((values, ctx) => {
        if (values.clearance !== "ValidUser") {
            return;
        }

        if (values.permissions.length === 0) {
            ctx.addIssue({ code: "custom", path: ["permissions"], message: "Grant access to at least one app" });
        }

        reportPermissionRowIssues(values.permissions, ctx, ["permissions"]);
    });

type GenerateCertificateFormData = z.infer<typeof generateCertificateSchema>;

function downloadBlob(blob: Blob, filename: string) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
}

export function GenerateCertificateDialog({ apps, trigger }: { apps: AppResponse[]; trigger: ReactNode }) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <GuardedDialog open={isOpen} onOpenChange={setIsOpen}>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="sm:max-w-lg">
                <DialogHeader>
                    <DialogTitle>Generate client certificate</DialogTitle>
                    <DialogDescription>
                        Create a client certificate for authenticating against the underlying RavenDB server. The
                        certificate downloads as a zip archive — the private key inside is not stored on the server, so
                        keep it safe.
                    </DialogDescription>
                </DialogHeader>
                {/* Rendered only while open, so a discarded draft resets on the next open. */}
                <GenerateCertificateForm apps={apps} onGenerated={() => setIsOpen(false)} />
            </DialogContent>
        </GuardedDialog>
    );
}

function GenerateCertificateForm({ apps, onGenerated }: { apps: AppResponse[]; onGenerated: () => void }) {
    const queryClient = useQueryClient();

    const form = useForm<GenerateCertificateFormData>({
        resolver: zodResolver(generateCertificateSchema),
        defaultValues: {
            name: "",
            password: "",
            clearance: "ValidUser",
            permissions: [{ database: "", access: "ReadWrite" }],
        },
    });
    const permissionRows = useFieldArray({ control: form.control, name: "permissions" });
    const clearance = useWatch({ control: form.control, name: "clearance" });
    const unsavedChanges = useFormUnsavedChanges(form);

    const generateMutation = useMutation({
        mutationFn: (values: GenerateCertificateFormData) =>
            api.services.certificates.generate({
                name: values.name,
                clearance: values.clearance,
                password: values.password || undefined,
                permissions: values.clearance === "ValidUser" ? toPermissionsRecord(values.permissions) : {},
            }),
        onSuccess: async (zip, values) => {
            unsavedChanges.markSaved();
            // Same filename the server sets in its Content-Disposition header.
            downloadBlob(zip, `${values.name}_certificates.zip`);
            toast.success(`Certificate “${values.name}” downloaded.`);
            await queryClient.invalidateQueries({ queryKey: api.queries.certificates.list().queryKey });
            onGenerated();
        },
    });

    const submit = form.handleSubmit((values) => generateMutation.mutate(values));

    const databaseOptions = apps.map((app) => toDatabaseOption(app.database, apps));
    const permissionsError = form.formState.errors.permissions;
    const permissionsErrorMessage = permissionsError?.root?.message ?? permissionsError?.message;

    return (
        <form className="grid gap-4" onSubmit={submit}>
            <FormInput control={form.control} name="name" label="Certificate name" placeholder="e.g. backups" />
            <FormInput control={form.control} name="password" type="password" label="Certificate password (optional)" />
            <FormSelect
                control={form.control}
                name="clearance"
                label="Security clearance"
                options={CLEARANCE_OPTIONS}
            />

            {clearance === "ValidUser" && (
                <div className="grid gap-3">
                    <Text as="div" variant="label">
                        App access
                    </Text>
                    {permissionRows.fields.map((row, index) => (
                        <div key={row.id} className="flex items-start gap-2">
                            <FormSelect
                                control={form.control}
                                name={`permissions.${index}.database`}
                                placeholder="Select an app"
                                options={databaseOptions}
                                className="flex-1"
                            />
                            <FormSelect
                                control={form.control}
                                name={`permissions.${index}.access`}
                                options={DATABASE_ACCESS_OPTIONS}
                                className="w-32 shrink-0"
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                aria-label="Remove access"
                                onClick={() => permissionRows.remove(index)}
                            >
                                <Trash2 className="size-4" aria-hidden="true" />
                            </Button>
                        </div>
                    ))}
                    {permissionsErrorMessage && <p className="text-sm text-destructive">{permissionsErrorMessage}</p>}
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="w-fit"
                        onClick={() => permissionRows.append({ database: "", access: "ReadWrite" })}
                    >
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add access
                    </Button>
                </div>
            )}

            {generateMutation.isError && (
                <Alert variant="destructive">
                    {generateMutation.error instanceof Error
                        ? generateMutation.error.message
                        : "Could not generate the certificate."}
                </Alert>
            )}

            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button type="submit" disabled={generateMutation.isPending}>
                    {generateMutation.isPending && <Spinner />}
                    Generate & download
                </Button>
            </DialogFooter>
        </form>
    );
}
