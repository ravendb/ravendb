import { useId, useRef, useState, type DragEvent, type ReactNode } from "react";
import { ImageIcon, Trash2 } from "lucide-react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { InfoHint } from "@/components/data/info-hint";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";

/** Raster formats only: an SVG data URI can carry script, and the widget CSP would let it through as data:. */
const ACCEPTED_TYPES = "image/png,image/jpeg,image/webp";

/** Reads the file, downscales it on a canvas, and returns a PNG data URI small enough to store inline. */
async function toDataUri(file: File, maxDimension: number): Promise<string> {
    const bitmap = await createImageBitmap(file);
    try {
        const scale = Math.min(1, maxDimension / Math.max(bitmap.width, bitmap.height));
        const canvas = document.createElement("canvas");
        canvas.width = Math.max(1, Math.round(bitmap.width * scale));
        canvas.height = Math.max(1, Math.round(bitmap.height * scale));

        const context = canvas.getContext("2d");
        if (context === null) throw new Error("canvas 2d context unavailable");
        context.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
        return canvas.toDataURL("image/png");
    } finally {
        bitmap.close();
    }
}

type FormImagePickerProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    /** Sits behind a help icon next to the label, for the format and size rules. */
    hint?: string;
    label?: ReactNode;
    /** The stored image is downscaled to fit this box, so the data URI stays small. */
    maxDimension?: number;
};

/**
 * An image field storing a small data URI: upload with client-side downscaling, thumbnail, remove.
 *
 * Laid out as one row - thumbnail, name, action - rather than a stack, so a panel of these reads as a
 * list of slots to fill. The thumbnail doubles as the drop target and the click-to-browse control,
 * which is what its dashed empty state has always looked like.
 */
export function FormImagePicker<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    hint,
    label,
    maxDimension = 128,
    name,
}: FormImagePickerProps<TFieldValues, TName>) {
    const id = useId();
    const inputRef = useRef<HTMLInputElement>(null);
    const [readError, setReadError] = useState<string | null>(null);
    const [isDragActive, setIsDragActive] = useState(false);
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
    } = useController({ control, defaultValue, name });

    const hasImage = typeof value === "string" && value.length > 0;

    const onFileSelected = async (file: File | null | undefined) => {
        if (!file) return;
        try {
            onChange(await toDataUri(file, maxDimension));
            setReadError(null);
        } catch {
            setReadError("Could not read that image. Use a png, jpeg or webp file.");
        }
    };

    const onDrop = (event: DragEvent<HTMLButtonElement>) => {
        if (disabled) return;
        event.preventDefault();
        setIsDragActive(false);
        void onFileSelected(event.dataTransfer.files.item(0));
    };

    return (
        <Field className={className} data-invalid={invalid || undefined}>
            <div className="flex items-center gap-3 rounded-lg border p-3">
                <button
                    type="button"
                    disabled={disabled}
                    aria-label={hasImage ? "Replace image" : "Upload image"}
                    onClick={() => inputRef.current?.click()}
                    onDragOver={(event) => {
                        if (disabled) return;
                        event.preventDefault();
                        setIsDragActive(true);
                    }}
                    onDragLeave={() => setIsDragActive(false)}
                    onDrop={onDrop}
                    className={cn(
                        "flex size-10 shrink-0 items-center justify-center overflow-hidden rounded-md border text-muted-foreground transition-colors focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none",
                        !hasImage && "border-dashed",
                        !disabled && "hover:border-ring hover:bg-accent",
                        isDragActive && !disabled && "border-primary-strong bg-primary/5",
                        disabled && "cursor-not-allowed opacity-60",
                    )}
                >
                    {hasImage ? (
                        <img src={value} alt="" className="size-full object-contain" />
                    ) : (
                        <ImageIcon className="size-5" aria-hidden="true" />
                    )}
                </button>
                <div className="grid min-w-0 flex-1 gap-0.5">
                    {label != null && (
                        <span className="flex items-center gap-1.5">
                            <FieldLabel htmlFor={id}>{label}</FieldLabel>
                            {hint && <InfoHint content={hint} />}
                        </span>
                    )}
                    {description && <FieldDescription className="text-xs">{description}</FieldDescription>}
                </div>
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={disabled}
                    onClick={() => inputRef.current?.click()}
                >
                    {hasImage ? "Replace" : "Upload"}
                </Button>
                {hasImage && (
                    <Button
                        type="button"
                        variant="ghost"
                        size="icon-sm"
                        aria-label="Remove image"
                        title="Remove image"
                        disabled={disabled}
                        onClick={() => {
                            onChange("");
                            setReadError(null);
                        }}
                    >
                        <Trash2 aria-hidden="true" />
                    </Button>
                )}
            </div>
            <input
                ref={inputRef}
                id={id}
                type="file"
                accept={ACCEPTED_TYPES}
                disabled={disabled}
                className="sr-only"
                onChange={(event) => {
                    void onFileSelected(event.target.files?.item(0));
                    // Reset so selecting the same file again still fires onChange.
                    event.target.value = "";
                }}
            />
            {(readError ?? error?.message) && (
                <FieldDescription className="text-destructive">{readError ?? error?.message}</FieldDescription>
            )}
        </Field>
    );
}
