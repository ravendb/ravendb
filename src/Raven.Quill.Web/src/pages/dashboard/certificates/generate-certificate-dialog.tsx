import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
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
import { toDatabaseOption } from "@/pages/dashboard/certificates/certificate-labels";

const generateCertificateSchema = z.object({
    name: z.string().trim().min(1, "Required"),
    appName: z.string().min(1, "Select an app"),
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
        defaultValues: { name: "", appName: "" },
    });

    const generateMutation = useMutation({
        mutationFn: (values: GenerateCertificateFormData) =>
            api.services.certificates.generate({ appName: values.appName, name: values.name }),
        onSuccess: async (zip, values) => {
            // Same filename the server sets in its Content-Disposition header.
            downloadBlob(zip, `${values.appName}_${values.name}_certificates.zip`);
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

    return (
        <Dialog open={isOpen} onOpenChange={handleOpenChange}>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className="sm:max-w-md">
                <DialogHeader>
                    <DialogTitle>Generate client certificate</DialogTitle>
                    <DialogDescription>
                        Create a client certificate with admin access to one app. The certificate downloads as a zip
                        archive — the private key inside is not stored on the server, so keep it safe.
                    </DialogDescription>
                </DialogHeader>

                <form className="grid gap-4" onSubmit={submit}>
                    <FormInput control={form.control} name="name" label="Certificate name" placeholder="e.g. backups" />
                    <FormSelect
                        control={form.control}
                        name="appName"
                        label="App"
                        placeholder="Select an app"
                        options={apps.map((app) => toDatabaseOption(app.database, apps))}
                        description="The certificate is granted admin access to this app's database."
                    />

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
