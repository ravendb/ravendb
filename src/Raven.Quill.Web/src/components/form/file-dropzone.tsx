import { useRef, useState, type ReactNode } from "react";
import { UploadIcon } from "lucide-react";
import { cn } from "@/lib/utils";
import { Text } from "@/components/typography";

type FileDropzoneProps = {
    /** Called with the first selected/dropped file. */
    onFileSelected: (file: File) => void;
    /** Native `accept` filter, e.g. "application/json,.json". */
    accept?: string;
    disabled?: boolean;
    className?: string;
    icon?: ReactNode;
    title?: ReactNode;
    description?: ReactNode;
};

/** Presentational drag & drop / click-to-browse file picker. It is callback-based rather than
 * react-hook-form bound, so it can back an import action as well as a form field. */
export function FileDropzone({
    onFileSelected,
    accept,
    disabled = false,
    className,
    icon = <UploadIcon className="size-6" aria-hidden="true" />,
    title = "Drag & drop a file here",
    description = "or click to browse",
}: FileDropzoneProps) {
    const inputRef = useRef<HTMLInputElement>(null);
    const [isDragActive, setIsDragActive] = useState(false);

    const emitFile = (files: FileList | null) => {
        const file = files?.item(0);

        if (file) {
            onFileSelected(file);
        }
    };

    const openPicker = () => {
        if (!disabled) {
            inputRef.current?.click();
        }
    };

    return (
        <div
            role="button"
            tabIndex={disabled ? -1 : 0}
            aria-disabled={disabled}
            onClick={openPicker}
            onKeyDown={(event) => {
                if (!disabled && (event.key === "Enter" || event.key === " ")) {
                    event.preventDefault();
                    openPicker();
                }
            }}
            onDragOver={(event) => {
                if (!disabled) {
                    event.preventDefault();
                    setIsDragActive(true);
                }
            }}
            onDragLeave={() => setIsDragActive(false)}
            onDrop={(event) => {
                if (!disabled) {
                    event.preventDefault();
                    setIsDragActive(false);
                    emitFile(event.dataTransfer.files);
                }
            }}
            className={cn(
                "flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-input bg-muted/30 px-6 py-8 text-center transition-colors outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50",
                !disabled && "cursor-pointer hover:bg-muted/60",
                isDragActive && !disabled && "border-primary-strong bg-primary/5",
                disabled && "cursor-not-allowed opacity-60",
                className,
            )}
        >
            <span className="text-muted-foreground">{icon}</span>
            <div className="grid gap-0.5">
                <Text variant="label" as="span" className="text-foreground">
                    {title}
                </Text>
                {description && (
                    <Text variant="caption" as="span">
                        {description}
                    </Text>
                )}
            </div>
            <input
                ref={inputRef}
                type="file"
                accept={accept}
                disabled={disabled}
                className="sr-only"
                onChange={(event) => {
                    emitFile(event.target.files);
                    // Reset so selecting the same file again still fires onChange.
                    event.target.value = "";
                }}
            />
        </div>
    );
}
