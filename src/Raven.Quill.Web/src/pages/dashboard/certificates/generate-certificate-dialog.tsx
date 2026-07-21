import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm } from "react-hook-form";
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
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { DATABASE_ACCESS_OPTIONS, toDatabaseOption } from "@/pages/dashboard/certificates/certificate-labels";
import {
    permissionRowSchema,
    reportDuplicateDatabases,
    toPermissionsRecord,
} from "@/pages/dashboard/certificates/certificate-permissions";

const generateCertificateSchema = z.object({
    name: z.string().trim().min(1, "Required"),
    permissions: z
        .array(permissionRowSchema)
        .min(1, "Grant access to at least one app")
        .superRefine(reportDuplicateDatabases),
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
    const queryClient = useQueryClient();

    const form = useForm<GenerateCertificateFormData>({
        resolver: zodResolver(generateCertificateSchema),
        defaultValues: { name: "", permissions: [{ database: "", access: "Read" }] },
    });
    const permissionRows = useFieldArray({ control: form.control, name: "permissions" });

    const generateMutation = useMutation({
        mutationFn: (values: GenerateCertificateFormData) =>
            // Per-database permissions only apply to ValidUser certificates, and this
            // dialog manages nothing above that clearance.
            api.services.certificates.generate(toPermissionsRecord(values.permissions), {
                name: values.name,
                clearance: "ValidUser",
            }),
        onSuccess: async (zip, values) => {
            // Same filename the server sets in its Content-Disposition header.
            downloadBlob(zip, `${values.name}_certificates.zip`);
            toast.success(`Certificate “${values.name}” downloaded.`);
            await queryClient.invalidateQueries({ queryKey: api.queries.certificates.list().queryKey });
            handleOpenChange(false);
        },
    });

    const submit = form.handleSubmit((values) => generateMutation.mutate(values));

    const handleOpenChange = (open: boolean) => {
        setIsOpen(open);
        if (!open) {
            form.reset();
            generateMutation.reset();
        }
    };

    const databaseOptions = apps.map((app) => toDatabaseOption(app.database, apps));
    const permissionsError = form.formState.errors.permissions;
    const permissionsErrorMessage = permissionsError?.root?.message ?? permissionsError?.message;

    return (
        <Dialog open={isOpen} onOpenChange={handleOpenChange}>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-lg">
                <DialogHeader>
                    <DialogTitle>Generate client certificate</DialogTitle>
                    <DialogDescription>
                        Create a client certificate with access to the selected apps. The certificate downloads as a zip
                        archive — the private key inside is not stored on the server, so keep it safe.
                    </DialogDescription>
                </DialogHeader>

                <form className="grid gap-4" onSubmit={submit}>
                    <FormInput control={form.control} name="name" label="Certificate name" placeholder="e.g. backups" />

                    <div className="grid gap-3">
                        <div className="text-sm font-medium">App access</div>
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
                        {permissionsErrorMessage && (
                            <p className="text-sm text-destructive">{permissionsErrorMessage}</p>
                        )}
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
            </DialogContent>
        </Dialog>
    );
}
