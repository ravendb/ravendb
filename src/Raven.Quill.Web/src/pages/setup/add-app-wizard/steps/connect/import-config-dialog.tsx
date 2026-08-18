import { useState } from "react";
import { UploadIcon } from "lucide-react";
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
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { FileDropzone } from "@/components/form/file-dropzone";
import { WizardErrorAlert } from "@/components/form/wizard/wizard-error-alert";
import { useImportConfig } from "@/pages/setup/add-app-wizard/steps/connect/use-import-config";

type ImportConfigDialogProps = {
    disabled?: boolean;
    /** Tooltip on the disabled trigger, explaining why the import is unavailable. */
    disabledExplanation?: string;
};

export function ImportConfigDialog({ disabled, disabledExplanation }: ImportConfigDialogProps) {
    const [isOpen, setIsOpen] = useState(false);
    const { importMutation, progressLabel } = useImportConfig();

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
                importMutation.reset();
                setIsOpen(false);
            },
        });
    };

    const trigger = (
        <DialogTrigger asChild>
            <Button type="button" variant="outline" disabled={disabled}>
                <UploadIcon aria-hidden="true" />
                Import configuration
            </Button>
        </DialogTrigger>
    );

    return (
        <Dialog open={isOpen} onOpenChange={handleOpenChange}>
            {disabled && disabledExplanation ? (
                // The disabled button swallows pointer events, so the span carries the tooltip.
                <TooltipProvider>
                    <Tooltip>
                        <TooltipTrigger asChild>
                            <span>{trigger}</span>
                        </TooltipTrigger>
                        <TooltipContent>{disabledExplanation}</TooltipContent>
                    </Tooltip>
                </TooltipProvider>
            ) : (
                trigger
            )}
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
                        <p className="text-sm text-muted-foreground" aria-live="polite">
                            {progressLabel}
                        </p>
                    </div>
                ) : (
                    <FileDropzone
                        accept="application/json,.json"
                        onFileSelected={handleFileSelected}
                        title="Drag & drop a configuration file"
                        description="or click to browse (.json)"
                    />
                )}

                {importMutation.isError && <WizardErrorAlert error={importMutation.error} />}
            </DialogContent>
        </Dialog>
    );
}
