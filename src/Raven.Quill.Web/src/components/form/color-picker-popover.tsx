import { useId, useState, type ReactNode } from "react";
import { Pipette } from "lucide-react";
import { HexColorPicker } from "react-colorful";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { parseColor, rgbToHsl, hslToRgb, toHex, type ColorFormat, type Hsl, type Rgb } from "@/lib/color";
import { cn } from "@/lib/utils";

const FORMATS: readonly { value: ColorFormat; label: string }[] = [
    { value: "hex", label: "HEX" },
    { value: "rgb", label: "RGB" },
    { value: "hsl", label: "HSL" },
];

type Channel = { key: string; label: string; min: number; max: number };

// RGB and HSL each get one spinner per channel. Hex stays a single field, since a hex string is one
// value rather than three, and splitting it into per-nibble inputs would not read as "channels".
//
// `label` names the input for screen readers only - the row shows no visible caption, so the full word
// ("Red value", not "R") is what a screen-reader user actually hears in place of the missing text. The
// "value" suffix also keeps "Hue value" from colliding with react-colorful's own hue slider, which the
// library names "Hue" - a bare "Hue" here would resolve to two elements sharing that accessible name.
const RGB_CHANNELS: readonly Channel[] = [
    { key: "r", label: "Red value", min: 0, max: 255 },
    { key: "g", label: "Green value", min: 0, max: 255 },
    { key: "b", label: "Blue value", min: 0, max: 255 },
];
const HSL_CHANNELS: readonly Channel[] = [
    { key: "h", label: "Hue value", min: 0, max: 360 },
    { key: "s", label: "Saturation value", min: 0, max: 100 },
    { key: "l", label: "Lightness value", min: 0, max: 100 },
];

const clamp = (n: number, min: number, max: number) => Math.min(max, Math.max(min, n));

type ColorPickerPopoverProps = {
    /** The committed colour, always a hex string. */
    value: string;
    onChange: (hex: string) => void;
    presets?: readonly string[];
    disabled?: boolean;
    /** Names the trigger for screen readers, e.g. "Button color" becomes "Button color picker". */
    label?: string;
    /** For callers that need a different swatch size or shape, e.g. a row of them in a settings list. */
    triggerClassName?: string;
    /** Puts the hex in the trigger beside the swatch, so a settings row can show its value without a
     *  field of its own and still open the picker from anywhere along it. */
    showValue?: boolean;
    /** Rendered at the start of the trigger, e.g. the row's own label - which is what makes the whole
     *  row, rather than just the swatch, the thing that opens the picker. */
    children?: ReactNode;
    /** Marks the trigger invalid for assistive tech when the caller's own validation has failed. */
    invalid?: boolean;
    /** Id of the element (e.g. a FieldDescription) that describes the error, wired onto the trigger via
     *  aria-describedby so the message is not orphaned from the control it explains. */
    describedBy?: string;
};

/**
 * The visual half of the colour picker: a saturation area, a hue strip, and per-format inputs that read
 * and write whichever of the three formats is selected. Presentational on purpose, so the form wiring
 * stays in FormColorPicker and this surface can be dropped anywhere a hex string is edited.
 */
export function ColorPickerPopover({
    children,
    value,
    onChange,
    presets,
    disabled,
    label,
    showValue,
    triggerClassName,
    invalid,
    describedBy,
}: ColorPickerPopoverProps) {
    const fieldId = useId();
    const [open, setOpen] = useState(false);
    const [format, setFormat] = useState<ColorFormat>("hex");
    const [draft, setDraft] = useState<string | null>(null);
    // Per-channel drafts for RGB/HSL, keyed by channel (e.g. "r", "h"). A record rather than a single
    // string because three independent fields can each be mid-edit at once, and each needs to keep its
    // own text without the other two re-deriving out from under it.
    const [channelDrafts, setChannelDrafts] = useState<Record<string, string>>({});
    // The last full RGB or HSL triple this popover itself committed, kept around after a channel field
    // blurs so that composing H, then S, then L (in any order) doesn't lose earlier fields to the hex
    // round trip's lossiness: once L reaches 0 the colour is black and H/S cannot be read back out of
    // it, so re-deriving every field from the committed hex after each blur would silently forget
    // whichever channel made the colour achromatic. Invalidated (by the `pendingValid` check below)
    // the moment something other than one of these commits changes the colour - a preset, the
    // eyedropper, Escape, or the popover opening fresh - so it never outlives the edit that built it.
    const [pending, setPending] = useState<Record<string, number> | null>(null);
    // The colour that was committed when the popover was last opened, so Escape can restore it. Every
    // other exit (outside click, the trigger) commits whatever is under the cursor; only Escape means
    // "never mind".
    const [openedValue, setOpenedValue] = useState<string | null>(null);

    const baseRgb: Rgb = parseColor(value) ?? { r: 0, g: 0, b: 0 };
    const baseHsl: Hsl = rgbToHsl(baseRgb);

    const pendingHex = (channels: Record<string, number>) =>
        format === "rgb" ? toHex(channels as unknown as Rgb) : toHex(hslToRgb(channels as unknown as Hsl));
    const pendingValid = pending !== null && pendingHex(pending) === value;

    const rgb: Rgb = format === "rgb" && pendingValid ? (pending as unknown as Rgb) : baseRgb;
    const hsl: Hsl = format === "hsl" && pendingValid ? (pending as unknown as Hsl) : baseHsl;

    // The hex field shows the committed colour except while the operator is typing into it. Deriving
    // the text on every keystroke would fight them: integer HSL cannot address every hex colour, so
    // "41" can round back to "40" mid-word and move the caret.
    const fieldValue = draft ?? toHex(baseRgb);
    // A half-typed or malformed draft (e.g. a paste that doesn't parse) is never committed, but it
    // should not vanish without a trace either.
    const draftInvalid = draft !== null && parseColor(draft, "hex") === null;

    const commit = (text: string) => {
        setDraft(text);
        const parsed = parseColor(text, "hex");
        if (parsed) onChange(toHex(parsed));
    };

    // Same hazard as the hex field, times three: a channel that is not focused shows the pending (or
    // committed) colour, so typing into R never disturbs G or B, and vice versa.
    const channelValue = (channel: Channel) =>
        channelDrafts[channel.key] ??
        String(format === "rgb" ? rgb[channel.key as keyof Rgb] : hsl[channel.key as keyof Hsl]);

    const channelInvalid = (channel: Channel) => {
        const text = channelDrafts[channel.key];
        return text !== undefined && (text.trim() === "" || Number.isNaN(Number(text)));
    };

    const commitChannel = (channel: Channel, text: string) => {
        setChannelDrafts((prev) => ({ ...prev, [channel.key]: text }));

        // An empty or unparseable channel is left alone rather than treated as 0: a cleared R field
        // must not commit black while the operator is still deciding what to type next.
        const num = Number(text);
        if (text.trim() === "" || Number.isNaN(num)) return;

        const base = format === "rgb" ? rgb : hsl;
        const next = { ...base, [channel.key]: clamp(num, channel.min, channel.max) };
        setPending(next);
        onChange(pendingHex(next));
    };

    // The draft(s) are cleared from the two event handlers that own them: switching format re-derives
    // every field from the committed value, and closing the popover (outside click, the trigger, or
    // Escape, which unmounts the fields without ever firing blur) drops whatever was left unparsed.
    // Either way a stale draft never survives past the interaction that created it. Pending channels
    // are cleared alongside them for the same reason - they are that format's in-progress composition,
    // not something the next tab or the next time this popover opens should inherit.
    const changeFormat = (next: string) => {
        if (!next) return;
        setFormat(next as ColorFormat);
        setDraft(null);
        setChannelDrafts({});
        setPending(null);
    };

    const changeOpen = (next: boolean) => {
        setOpen(next);
        if (next) {
            setOpenedValue(value);
        } else {
            setDraft(null);
            setChannelDrafts({});
            setPending(null);
        }
    };

    // Escape means cancel, not commit: it restores the colour that was committed when the popover
    // opened, undoing whatever the operator dragged or typed while looking around. Outside-click and
    // the trigger stay a commit, since only Escape carries that "never mind" meaning in this product.
    const cancel = () => {
        if (openedValue !== null) onChange(openedValue);
    };

    const swatch = parseColor(value) ? value : "#000000";
    const channels = format === "rgb" ? RGB_CHANNELS : format === "hsl" ? HSL_CHANNELS : null;

    return (
        <Popover open={open} onOpenChange={changeOpen}>
            {showValue ? (
                // No aria-label here: with the value rendered right in the trigger, an overriding label
                // would hide the one piece of information ("Button #ff775f") a screen-reader user
                // actually needs, that no other affordance in this row exposes.
                <PopoverTrigger
                    type="button"
                    disabled={disabled}
                    aria-invalid={invalid || undefined}
                    aria-describedby={describedBy}
                    className={cn(
                        "flex cursor-pointer items-center gap-2 rounded-sm transition-transform focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none active:scale-95 disabled:cursor-not-allowed",
                        triggerClassName,
                    )}
                >
                    {children}
                    <span className="flex items-center gap-2">
                        <span className="text-[0.78rem] font-medium text-muted-foreground tabular-nums">{value}</span>
                        <span
                            aria-hidden="true"
                            className="size-[1.125rem] shrink-0 rounded-full border border-border"
                            style={{ background: swatch }}
                        />
                    </span>
                </PopoverTrigger>
            ) : (
                <PopoverTrigger
                    type="button"
                    disabled={disabled}
                    aria-label={label ? `${label} picker` : "Color picker"}
                    aria-invalid={invalid || undefined}
                    aria-describedby={describedBy}
                    style={{ background: swatch }}
                    className={cn(
                        "size-5 shrink-0 cursor-pointer rounded-full ring-1 ring-foreground/20 ring-inset focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed",
                        triggerClassName,
                    )}
                />
            )}
            <PopoverContent
                align="start"
                className="w-64 gap-2.5 overflow-hidden p-0"
                aria-label={label ? `${label} picker` : "Color picker"}
                onEscapeKeyDown={cancel}
            >
                {/* One area for all three tabs. An earlier revision mounted HslColorPicker on the HSL
                    tab to give it a saturation-against-lightness surface, but react-colorful 5.8.0 has
                    a single area component: HslColorPicker converts to HSV and draws the identical
                    saturation/value square, so the swap changed nothing a user could see. The format
                    only decides which numbers you type. */}
                {/* Inset to the same 2.5 the rest of the popover uses, rather than bled to the edge.
                    react-colorful centres each handle on its position, so an edge-to-edge area clips
                    half the handle at every extreme: fully saturated, white, or a hue near 0. That
                    hides the one thing telling the operator where they are, exactly when they are at a
                    limit. The handles are scaled down in index.css to fit within this inset. */}
                <div className="rq-color-area px-2.5 pt-2.5">
                    <HexColorPicker color={swatch} onChange={onChange} className="!w-full" />
                </div>

                <div className="flex flex-col gap-2.5 px-2.5 pb-2.5">
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

                    <div className="flex items-center gap-1.5">
                        {channels ? (
                            channels.map((channel) => (
                                <Field key={channel.key} className="flex-1">
                                    {/* No visible caption - the row is just the inputs and the pipette - but the
                                        input still needs a name of its own, since "one field called Color value"
                                        no longer describes three boxes. */}
                                    <FieldLabel htmlFor={`${fieldId}-${channel.key}`} className="sr-only">
                                        {channel.label}
                                    </FieldLabel>
                                    <input
                                        id={`${fieldId}-${channel.key}`}
                                        type="number"
                                        min={channel.min}
                                        max={channel.max}
                                        step={1}
                                        disabled={disabled}
                                        aria-invalid={channelInvalid(channel) || undefined}
                                        value={channelValue(channel)}
                                        onChange={(event) => commitChannel(channel, event.target.value)}
                                        onBlur={() =>
                                            setChannelDrafts((prev) =>
                                                Object.fromEntries(
                                                    Object.entries(prev).filter(([key]) => key !== channel.key),
                                                ),
                                            )
                                        }
                                        className="h-8 w-full [appearance:textfield] rounded-md border bg-background px-1.5 text-center font-mono text-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
                                    />
                                </Field>
                            ))
                        ) : (
                            <Field className="flex-1">
                                <FieldLabel htmlFor={fieldId} className="sr-only">
                                    HEX value
                                </FieldLabel>
                                <input
                                    id={fieldId}
                                    type="text"
                                    spellCheck={false}
                                    autoComplete="off"
                                    value={fieldValue}
                                    disabled={disabled}
                                    aria-invalid={draftInvalid || undefined}
                                    onChange={(event) => commit(event.target.value)}
                                    onBlur={() => setDraft(null)}
                                    className="h-8 w-full rounded-md border bg-background px-2 font-mono text-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
                                />
                            </Field>
                        )}

                        <EyedropperButton disabled={disabled} onPick={onChange} />
                    </div>

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
                </div>
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
        <Button
            type="button"
            variant="outline"
            size="icon"
            disabled={disabled}
            onClick={pick}
            aria-label="Pick from screen"
            className="shrink-0"
        >
            <Pipette className="size-3.5" />
        </Button>
    );
}
