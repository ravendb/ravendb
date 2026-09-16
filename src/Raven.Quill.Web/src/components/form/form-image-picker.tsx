import { useId, useRef, useState, type ReactNode } from "react";
import { ImageIcon } from "lucide-react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";

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
    label?: ReactNode;
    /** The stored image is downscaled to fit this box, so the data URI stays small. */
    maxDimension?: number;
};

/** An image field storing a small data URI: upload with client-side downscaling, thumbnail, remove. */
export function FormImagePicker<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    maxDimension = 128,
    name,
}: FormImagePickerProps<TFieldValues, TName>) {
    const id = useId();
    const inputRef = useRef<HTMLInputElement>(null);
    const [readError, setReadError] = useState<string | null>(null);
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

    return (
        <Field className={className} data-invalid={invalid || undefined}>
            {label && <FieldLabel htmlFor={id}>{label}</FieldLabel>}
            <div className="flex items-center gap-3">
                {hasImage ? (
                    <img src={value} alt="" className="size-12 shrink-0 rounded-md border object-contain" />
                ) : (
                    <span className="flex size-12 shrink-0 items-center justify-center rounded-md border border-dashed text-muted-foreground">
                        <ImageIcon className="size-5" aria-hidden="true" />
                    </span>
                )}
                <div className="flex gap-2">
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={disabled}
                        onClick={() => inputRef.current?.click()}
                    >
                        {hasImage ? "Replace" : "Upload image"}
                    </Button>
                    {hasImage && (
                        <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            disabled={disabled}
                            onClick={() => {
                                onChange("");
                                setReadError(null);
                            }}
                        >
                            Remove
                        </Button>
                    )}
                </div>
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
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
