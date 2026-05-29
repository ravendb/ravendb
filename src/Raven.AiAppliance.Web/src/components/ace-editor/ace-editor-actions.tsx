import { useId, type ReactNode } from "react";
import { Dialog } from "radix-ui";
import { AlignLeft, ChevronsUpDown, CircleHelp, Maximize2, Trash2, Upload, X } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { handleAutoResizeHeight, handleFormat } from "@/components/ace-editor/ace-editor-action-utils";
import { useAceEditorContext } from "@/components/ace-editor/ace-editor-context";
import { cn } from "@/lib/utils";

type IconButtonProps = {
    children: ReactNode;
    className?: string;
    onClick?: () => void;
    title: string;
};

function AceEditorIconButton({ children, className, onClick, title }: IconButtonProps) {
    return (
        <Button
            aria-label={title}
            className={cn("size-7", className)}
            onClick={onClick}
            size="icon-sm"
            title={title}
            type="button"
            variant="ghost"
        >
            {children}
        </Button>
    );
}

export function AceEditorFullScreenAction() {
    const { aceRef, rootRef } = useAceEditorContext();

    function handleFullScreen() {
        const fullScreenElement = rootRef.current ?? aceRef.current?.editor.container;
        void fullScreenElement?.requestFullscreen();
        window.setTimeout(() => aceRef.current?.editor.resize(), 0);
    }

    return (
        <AceEditorIconButton onClick={handleFullScreen} title="Full screen">
            <Maximize2 />
        </AceEditorIconButton>
    );
}

export function AceEditorFormatAction() {
    const { aceRef } = useAceEditorContext();

    return (
        <AceEditorIconButton onClick={() => handleFormat(aceRef)} title="Format">
            <AlignLeft />
        </AceEditorIconButton>
    );
}

type AceEditorLoadFileActionProps = {
    onLoad: (content: string) => void;
};

export function AceEditorLoadFileAction({ onLoad }: AceEditorLoadFileActionProps) {
    const id = useId();

    function handleFileChange(event: React.ChangeEvent<HTMLInputElement>) {
        const file = event.currentTarget.files?.[0];

        if (!file) {
            return;
        }

        const reader = new FileReader();
        reader.onload = () => onLoad(String(reader.result ?? ""));
        reader.readAsText(file);
        event.currentTarget.value = "";
    }

    return (
        <div>
            <label
                aria-label="Load from a file"
                className="inline-flex size-7 items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground"
                htmlFor={id}
                title="Load from a file"
            >
                <Upload className="size-4" />
            </label>
            <input className="sr-only" id={id} onChange={handleFileChange} type="file" />
        </div>
    );
}

type AceEditorDeleteActionProps = {
    onDelete: () => void;
};

export function AceEditorDeleteAction({ onDelete }: AceEditorDeleteActionProps) {
    return (
        <AceEditorIconButton onClick={onDelete} title="Delete">
            <Trash2 />
        </AceEditorIconButton>
    );
}

type AceEditorHelpActionProps = {
    message: ReactNode;
    title?: string;
    tooltipTitle?: string;
};

export function AceEditorHelpAction({
    message,
    title = "Syntax help",
    tooltipTitle = "Syntax help",
}: AceEditorHelpActionProps) {
    return (
        <Dialog.Root>
            <Dialog.Trigger asChild>
                <AceEditorIconButton title={tooltipTitle}>
                    <CircleHelp />
                </AceEditorIconButton>
            </Dialog.Trigger>
            <Dialog.Portal>
                <Dialog.Overlay className="fixed inset-0 z-50 bg-background/80 backdrop-blur-sm" />
                <Dialog.Content className="fixed top-1/2 left-1/2 z-50 grid max-h-[85vh] w-[min(42rem,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 gap-4 overflow-auto rounded-lg border bg-popover p-5 text-popover-foreground shadow-lg">
                    <div className="flex items-start justify-between gap-4">
                        <Dialog.Title className="text-base font-semibold">{title}</Dialog.Title>
                        <Dialog.Close asChild>
                            <Button aria-label="Close" size="icon-sm" type="button" variant="ghost">
                                <X />
                            </Button>
                        </Dialog.Close>
                    </div>
                    <div className="text-sm text-muted-foreground">{message}</div>
                </Dialog.Content>
            </Dialog.Portal>
        </Dialog.Root>
    );
}

export function AceEditorAutoResizeHeightAction() {
    const { aceRef, setHeight } = useAceEditorContext();

    return (
        <AceEditorIconButton onClick={() => handleAutoResizeHeight(aceRef, setHeight)} title="Resize to content">
            <ChevronsUpDown />
        </AceEditorIconButton>
    );
}
