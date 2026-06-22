import { useState } from "react";
import { toast } from "sonner";
import { UploadIcon } from "lucide-react";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/shadcn/ui/dialog";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { FileDropzone } from "@/components/form/file-dropzone";
import { useImportConfig } from "@/pages/setup/add-app-wizard/steps/connect/use-import-config";

type ImportConfigDialogProps = {
    disabled?: boolean;
};

export function ImportConfigDialog({ disabled }: ImportConfigDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const importMutation = useImportConfig();

    const handleOpenChange = (open: boolean) => {
        if (importMutation.isPending) {
            return;
        }

        if (!open) {
            importMutation.reset();
        }

        setIsOpen(open);
    };

    const handleFileSelected = (file: File) => {
        importMutation.mutate(file, {
            onSuccess: () => {
                setIsOpen(false);
                importMutation.reset();
                toast.success("Configuration imported");
            },
        });
    };

    return (
        <Dialog open={isOpen} onOpenChange={handleOpenChange}>
            <DialogTrigger asChild>
                <Button type="button" variant="outline" disabled={disabled}>
                    <UploadIcon aria-hidden="true" />
                    Import configuration
                </Button>
            </DialogTrigger>
            <DialogContent showCloseButton={!importMutation.isPending}>
                <DialogHeader>
                    <DialogTitle>Import configuration</DialogTitle>
                    <DialogDescription>
                        Load a previously exported configuration. The connection and every table are verified against
                        your source database before the wizard is filled in.
                    </DialogDescription>
                </DialogHeader>

                {importMutation.isPending ? (
                    <div className="flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed px-6 py-8 text-center">
                        <Spinner className="size-6" />
                        <p className="text-sm text-muted-foreground">Validating the connection and verifying tables…</p>
                    </div>
                ) : (
                    <FileDropzone
                        accept="application/json,.json"
                        onFileSelected={handleFileSelected}
                        title="Drag & drop a configuration file"
                        description="or click to browse (.json)"
                    />
                )}

                {importMutation.isError && (
                    <Alert variant="destructive" className="whitespace-pre-wrap">
                        {importMutation.error.message}
                    </Alert>
                )}
            </DialogContent>
        </Dialog>
    );
}
