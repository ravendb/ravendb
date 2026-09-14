import { useState, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Download, ExternalLink, Plus, Trash2 } from "lucide-react";
import { z } from "zod";
import { api } from "@/api/api";
import type { AppResponse } from "@/api/generated/server-api";
import { InfoHint } from "@/components/data/info-hint";
import { InlineCode } from "@/components/data/inline-code";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
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
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { GuardedDialog } from "@/components/form/unsaved-changes/guarded-overlays";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text, textVariants } from "@/components/typography";
import { CLIENT_ACCESS_DOCS_URL } from "@/lib/help-links";
import { cn } from "@/lib/utils";
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
type GeneratedCertificate = {
    name: string;
    zip: Blob;
};

const CERTIFICATE_NAME_HELP =
    "Identifies the certificate in Quill and RavenDB and is used in the downloaded filenames. Generating with an existing name creates a separate certificate; it does not replace the existing one.";
const CERTIFICATE_PASSWORD_HELP =
    "Optional. Protects only the .pfx file. The ZIP archive and .key file are not encrypted. The password cannot be recovered.";
const SECURITY_CLEARANCE_HELP =
    "User is limited to the selected apps. Operator can access every RavenDB database and perform server-wide operations.";
const APP_ACCESS_HELP =
    "For a User certificate, select the apps it can access. “Read/Write” permits data operations; “Admin” also permits database configuration.";

function downloadBlob(blob: Blob, filename: string) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

export function GenerateCertificateDialog({ apps, trigger }: { apps: AppResponse[]; trigger: ReactNode }) {
    const [isOpen, setIsOpen] = useState(false);
    const [generatedCertificate, setGeneratedCertificate] = useState<GeneratedCertificate | null>(null);

    function handleOpenChange(open: boolean) {
        setIsOpen(open);
        if (!open) {
            setGeneratedCertificate(null);
        }
    }

    return (
        <GuardedDialog open={isOpen} onOpenChange={handleOpenChange}>
            <DialogTrigger asChild>{trigger}</DialogTrigger>
            <DialogContent className={generatedCertificate ? "sm:max-w-xl" : "sm:max-w-lg"}>
                {generatedCertificate ? (
                    <CertificateGenerated certificate={generatedCertificate} />
                ) : (
                    <>
                        <DialogHeader className="gap-3">
                            <DialogTitle>Generate client certificate</DialogTitle>
                            <DialogDescription>
                                Generate a client certificate to access RavenDB databases associated with Quill apps,
                                either from an application using the RavenDB Client API or from RavenDB Studio in a
                                browser.
                                <span className="mt-2 block">
                                    Grant only the app access and permissions required for the intended use. The private
                                    key is downloaded once and cannot be recovered from the server.
                                </span>
                            </DialogDescription>
                        </DialogHeader>
                        {/* Rendered only while open, so a discarded draft resets on the next open. */}
                        <GenerateCertificateForm apps={apps} onGenerated={setGeneratedCertificate} />
                    </>
                )}
            </DialogContent>
        </GuardedDialog>
    );
}

function CertificateGenerated({ certificate }: { certificate: GeneratedCertificate }) {
    const filename = `${certificate.name}_certificates.zip`;

    return (
        <>
            <DialogHeader className="gap-3">
                <DialogTitle>Certificate generated</DialogTitle>
                <DialogDescription>
                    <InlineCode>{filename}</InlineCode> was downloaded.
                    <span className="block">
                        Store the ZIP file and its contents securely. The certificate is valid for five years.
                    </span>
                </DialogDescription>
            </DialogHeader>

            <div className="space-y-4">
                <div className="space-y-2">
                    <Text as="div" variant="label">
                        How to use the downloaded files
                    </Text>
                    <ul className={cn(textVariants({ variant: "muted" }), "list-disc space-y-2 pl-5")}>
                        <li>
                            Use the <InlineCode>.pfx</InlineCode> file with clients that accept the certificate and
                            private key in one file, or install it for browser access to RavenDB Studio.
                        </li>
                        <li>
                            Use the <InlineCode>.crt</InlineCode> and <InlineCode>.key</InlineCode> together with
                            clients or tools that require PEM files.
                        </li>
                    </ul>
                </div>

                <Alert variant="warning">
                    <AlertTitle className="mb-2">Store this download securely</AlertTitle>
                    <AlertDescription>
                        <span className="block">
                            Quill cannot recover this ZIP or private key after you close this dialog. The optional
                            password protects only the <InlineCode>.pfx</InlineCode> file; the ZIP archive and{" "}
                            <InlineCode>.key</InlineCode> file are not encrypted.
                        </span>
                        <span className="mt-2 block">
                            If the archive is lost or the private key is exposed, generate a replacement and disable the
                            old certificate from <strong>Certificates &gt; Edit</strong>. Disabling blocks new
                            connections, but existing connections remain active until they close.
                        </span>
                    </AlertDescription>
                </Alert>
            </div>

            <DialogFooter>
                <TooltipProvider>
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <Button
                                type="button"
                                variant="outline"
                                onClick={() => downloadBlob(certificate.zip, filename)}
                            >
                                <Download aria-hidden="true" />
                                Download ZIP again
                            </Button>
                        </TooltipTrigger>
                        <TooltipContent>
                            Download the same certificate ZIP again without generating a new certificate. This option is
                            available only until you close the dialog.
                        </TooltipContent>
                    </Tooltip>
                </TooltipProvider>
                <Button variant="outline" asChild>
                    <a href={CLIENT_ACCESS_DOCS_URL} target="_blank" rel="noreferrer">
                        Learn how to connect an application
                        <ExternalLink aria-hidden="true" />
                    </a>
                </Button>
                <DialogClose asChild>
                    <Button type="button">Done</Button>
                </DialogClose>
            </DialogFooter>
        </>
    );
}

function GenerateCertificateForm({
    apps,
    onGenerated,
}: {
    apps: AppResponse[];
    onGenerated: (certificate: GeneratedCertificate) => void;
}) {
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
            onGenerated({ name: values.name, zip });
            await queryClient.invalidateQueries({ queryKey: api.queries.certificates.list().queryKey });
        },
    });

    const submit = form.handleSubmit((values) => generateMutation.mutate(values));

    const databaseOptions = apps.map((app) => toDatabaseOption(app.database, apps));
    const permissionsError = form.formState.errors.permissions;
    const permissionsErrorMessage = permissionsError?.root?.message ?? permissionsError?.message;

    return (
        <form className="grid gap-4" onSubmit={submit}>
            {/* autoFocus keeps the dialog's opening focus here: the label's InfoHint button precedes the
                input in the DOM, so the first tabbable element would otherwise be the hint, whose tooltip
                then opens on focus. */}
            <FormInput
                control={form.control}
                name="name"
                label="Certificate name"
                labelAddon={<InfoHint content={CERTIFICATE_NAME_HELP} />}
                placeholder="e.g. acme-shop-api"
                autoFocus
            />
            <FormInput
                control={form.control}
                name="password"
                type="password"
                label="Certificate password (optional)"
                labelAddon={<InfoHint content={CERTIFICATE_PASSWORD_HELP} />}
            />
            <FormSelect
                control={form.control}
                name="clearance"
                label="Security clearance"
                labelAddon={<InfoHint content={SECURITY_CLEARANCE_HELP} />}
                options={CLEARANCE_OPTIONS}
            />

            {clearance === "ValidUser" && (
                <div className="grid gap-3">
                    <Text as="div" variant="label" className="flex items-center gap-1">
                        App access <InfoHint content={APP_ACCESS_HELP} />
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
