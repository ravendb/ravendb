import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import type { CertificateItem } from "@/api/custom-services/certificates-service";
import type { AppResponse } from "@/api/generated/server-api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
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

const editCertificateSchema = z
    .object({
        name: z.string().trim().min(1, "Required"),
        isEnabled: z.boolean(),
        clearance: z.enum(["Operator", "ValidUser"]),
        permissions: z.array(permissionRowSchema),
    })
    .superRefine((values, ctx) => {
        if (values.clearance === "ValidUser") {
            reportPermissionRowIssues(values.permissions, ctx, ["permissions"]);
        }
    });

type EditCertificateFormData = z.infer<typeof editCertificateSchema>;

export function EditCertificateDialog({
    certificate,
    apps,
    trigger,
}: {
    certificate: CertificateItem;
    apps: AppResponse[];
    trigger: ReactNode;
}) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <GuardedDialog open={isOpen} onOpenChange={setIsOpen}>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="sm:max-w-lg">
                <DialogHeader>
                    <DialogTitle>Edit certificate</DialogTitle>
                    <DialogDescription>
                        Rename the certificate, enable or disable it, change its security clearance, and manage which
                        apps it can access.
                    </DialogDescription>
                </DialogHeader>
                {/* Rendered only while open: each open seeds fresh and a discarded draft doesn't linger. */}
                <EditCertificateForm certificate={certificate} apps={apps} onSaved={() => setIsOpen(false)} />
            </DialogContent>
        </GuardedDialog>
    );
}

function EditCertificateForm({
    certificate,
    apps,
    onSaved,
}: {
    certificate: CertificateItem;
    apps: AppResponse[];
    onSaved: () => void;
}) {
    const queryClient = useQueryClient();

    const form = useForm<EditCertificateFormData>({
        resolver: zodResolver(editCertificateSchema),
        defaultValues: toFormData(certificate),
    });
    const permissionRows = useFieldArray({ control: form.control, name: "permissions" });
    const clearance = useWatch({ control: form.control, name: "clearance" });
    const unsavedChanges = useFormUnsavedChanges(form);

    const editMutation = useMutation({
        mutationFn: (values: EditCertificateFormData) =>
            api.services.settings.certificatesEdit(
                values.clearance === "ValidUser" ? toPermissionsRecord(values.permissions) : {},
                {
                    thumbprint: certificate.thumbprint,
                    name: values.name,
                    clearance: values.clearance,
                    disable: !values.isEnabled,
                },
            ),
        onSuccess: async (_, values) => {
            unsavedChanges.markSaved();
            toast.success(`Certificate “${values.name}” updated.`);
            await queryClient.invalidateQueries({ queryKey: api.queries.certificates.list().queryKey });
            onSaved();
        },
    });

    const submit = form.handleSubmit((values) => editMutation.mutate(values));

    // Databases the certificate already references stay selectable even when they
    // don't match a current app (e.g. the app was removed).
    const knownDatabases = [
        ...new Set([...apps.map((app) => app.database), ...Object.keys(certificate.permissions ?? {})]),
    ];
    const databaseOptions = knownDatabases.map((database) => toDatabaseOption(database, apps));

    return (
        <form className="grid gap-4" onSubmit={submit}>
            <FormInput control={form.control} name="name" label="Certificate name" />
            <FormSwitch control={form.control} name="isEnabled" label="Enabled" />
            <FormSelect
                control={form.control}
                name="clearance"
                label="Security clearance"
                options={CLEARANCE_OPTIONS}
            />

            {clearance === "ValidUser" && (
                <div className="grid gap-3">
                    <div className="text-sm font-medium">App access</div>
                    {permissionRows.fields.length === 0 && (
                        <p className="text-sm text-muted-foreground">
                            No access granted — this certificate cannot reach any app.
                        </p>
                    )}
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
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="w-fit"
                        onClick={() => permissionRows.append({ database: "", access: "Read" })}
                    >
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add access
                    </Button>
                </div>
            )}

            {editMutation.isError && (
                <Alert variant="destructive">
                    {editMutation.error instanceof Error
                        ? editMutation.error.message
                        : "Could not update the certificate."}
                </Alert>
            )}

            <DialogFooter>
                <DialogClose asChild>
                    <Button type="button" variant="outline">
                        Cancel
                    </Button>
                </DialogClose>
                <Button type="submit" disabled={editMutation.isPending}>
                    {editMutation.isPending && <Spinner />}
                    Save changes
                </Button>
            </DialogFooter>
        </form>
    );
}

function toFormData(certificate: CertificateItem): EditCertificateFormData {
    return {
        name: certificate.name ?? "",
        isEnabled: !certificate.disabled,
        // isEditableCertificate limits this dialog to Operator and ValidUser.
        clearance: certificate.securityClearance === "Operator" ? "Operator" : "ValidUser",
        permissions: Object.entries(certificate.permissions ?? {}).map(([database, access]) => ({
            database,
            access,
        })),
    };
}
