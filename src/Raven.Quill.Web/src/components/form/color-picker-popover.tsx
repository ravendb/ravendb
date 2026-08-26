import { useId, useState } from "react";
import { Pipette } from "lucide-react";
import { HexColorPicker } from "react-colorful";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { formatAs, parseColor, toHex, type ColorFormat } from "@/lib/color";

const FORMATS: readonly { value: ColorFormat; label: string }[] = [
    { value: "hex", label: "HEX" },
    { value: "rgb", label: "RGB" },
    { value: "hsl", label: "HSL" },
];

type ColorPickerPopoverProps = {
    /** The committed colour, always a hex string. */
    value: string;
    onChange: (hex: string) => void;
    presets?: readonly string[];
    disabled?: boolean;
    /** Names the trigger for screen readers, e.g. "Button color" becomes "Button color picker". */
    label?: string;
};

/**
 * The visual half of the colour picker: a saturation area, a hue strip, and one field that reads and
 * writes whichever of the three formats is selected. Presentational on purpose, so the form wiring
 * stays in FormColorPicker and this surface can be dropped anywhere a hex string is edited.
 */
export function ColorPickerPopover({ value, onChange, presets, disabled, label }: ColorPickerPopoverProps) {
    const fieldId = useId();
    const [open, setOpen] = useState(false);
    const [format, setFormat] = useState<ColorFormat>("hex");
    const [draft, setDraft] = useState<string | null>(null);

    // The field shows the committed colour except while the operator is typing into it. Deriving the
    // text on every keystroke would fight them: integer HSL cannot address every hex colour, so "41"
    // can round back to "40" mid-word and move the caret.
    const fieldValue = draft ?? formatAs(format, value);

    const commit = (text: string) => {
        setDraft(text);
        const parsed = parseColor(text, format);
        if (parsed) onChange(toHex(parsed));
    };

    // The draft is cleared from the two event handlers that own it: switching format re-derives the
    // field from the committed value, and closing the popover (outside click, the trigger, or Escape,
    // which unmounts the field without ever firing blur) drops whatever was left unparsed. Either way
    // a stale draft never survives past the interaction that created it.
    const changeFormat = (next: string) => {
        if (!next) return;
        setFormat(next as ColorFormat);
        setDraft(null);
    };

    const changeOpen = (next: boolean) => {
        setOpen(next);
        if (!next) setDraft(null);
    };

    const swatch = parseColor(value) ? value : "#000000";
    const formatLabel = FORMATS.find((option) => option.value === format)?.label ?? "Color";

    return (
        <Popover open={open} onOpenChange={changeOpen}>
            <PopoverTrigger
                type="button"
                disabled={disabled}
                aria-label={label ? `${label} picker` : "Color picker"}
                style={{ background: swatch }}
                className="size-5 shrink-0 cursor-pointer rounded-full ring-1 ring-foreground/20 ring-inset focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed"
            />
            <PopoverContent align="start" className="w-64" aria-label={label ? `${label} picker` : "Color picker"}>
                <HexColorPicker color={swatch} onChange={onChange} />

                <ToggleGroup
                    type="single"
                    size="sm"
                    value={format}
                    onValueChange={changeFormat}
                    aria-label="Color format"
                    className="w-full"
                >
                    {FORMATS.map((option) => (
                        <ToggleGroupItem key={option.value} value={option.value} className="flex-1">
                            {option.label}
                        </ToggleGroupItem>
                    ))}
                </ToggleGroup>

                <Field>
                    <FieldLabel htmlFor={fieldId} className="sr-only">
                        {formatLabel} value
                    </FieldLabel>
                    <input
                        id={fieldId}
                        type="text"
                        spellCheck={false}
                        autoComplete="off"
                        value={fieldValue}
                        disabled={disabled}
                        onChange={(event) => commit(event.target.value)}
                        onBlur={() => setDraft(null)}
                        className="h-8 w-full rounded-md border bg-background px-2 font-mono text-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
                    />
                </Field>

                {presets && presets.length > 0 && (
                    <div className="flex flex-wrap gap-1.5">
                        {presets.map((preset) => (
                            <button
                                key={preset}
                                type="button"
                                disabled={disabled}
                                aria-label={`Use ${preset}`}
                                style={{ background: preset }}
                                onClick={() => onChange(preset)}
                                className="size-5 rounded-full ring-1 ring-foreground/20 transition-transform ring-inset hover:scale-110 focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed"
                            />
                        ))}
                    </div>
                )}

                <EyedropperButton disabled={disabled} onPick={onChange} />
            </PopoverContent>
        </Popover>
    );
}

/** Chromium only. Rendering nothing elsewhere beats rendering a control that cannot work. */
function EyedropperButton({ disabled, onPick }: { disabled?: boolean; onPick: (hex: string) => void }) {
    const [supported] = useState(() => typeof window !== "undefined" && "EyeDropper" in window);
    if (!supported) return null;

    const pick = async () => {
        try {
            // @ts-expect-error EyeDropper is not in TypeScript's DOM lib yet.
            const result = await new window.EyeDropper().open();
            const parsed = parseColor(result.sRGBHex);
            if (parsed) onPick(toHex(parsed));
        } catch {
            // The operator pressed Escape. Nothing to do, and nothing worth telling them.
        }
    };

    return (
        <Button type="button" variant="outline" size="sm" disabled={disabled} onClick={pick} className="w-full">
            <Pipette className="size-3.5" />
            Pick from screen
        </Button>
    );
}
