import { Autocomplete as AutocompletePrimitive } from "@base-ui/react";

import { cn } from "@/lib/utils";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { usePortalContainer } from "@/components/shadcn/ui/portal-container-context";
import { ChevronDownIcon, XIcon } from "lucide-react";

const Autocomplete = AutocompletePrimitive.Root;

function AutocompleteTrigger({ className, children, ...props }: AutocompletePrimitive.Trigger.Props) {
    return (
        <AutocompletePrimitive.Trigger
            data-slot="autocomplete-trigger"
            className={cn("[&_svg:not([class*='size-'])]:size-4", className)}
            {...props}
        >
            {children}
            <ChevronDownIcon className="pointer-events-none size-4 text-muted-foreground" />
        </AutocompletePrimitive.Trigger>
    );
}

function AutocompleteClear({ className, ...props }: AutocompletePrimitive.Clear.Props) {
    return (
        <AutocompletePrimitive.Clear
            data-slot="autocomplete-clear"
            render={<InputGroupButton variant="ghost" size="icon-xs" />}
            className={cn(className)}
            {...props}
        >
            <XIcon className="pointer-events-none" />
        </AutocompletePrimitive.Clear>
    );
}

function AutocompleteInput({
    className,
    children,
    disabled = false,
    showTrigger = true,
    showClear = false,
    ...props
}: AutocompletePrimitive.Input.Props & {
    showTrigger?: boolean;
    showClear?: boolean;
}) {
    return (
        <InputGroup className={cn("w-auto", className)}>
            <AutocompletePrimitive.Input render={<InputGroupInput disabled={disabled} />} {...props} />
            <InputGroupAddon align="inline-end">
                {showTrigger && (
                    <InputGroupButton
                        size="icon-xs"
                        variant="ghost"
                        asChild
                        data-slot="input-group-button"
                        className="group-has-data-[slot=autocomplete-clear]/input-group:hidden data-pressed:bg-transparent"
                        disabled={disabled}
                    >
                        <AutocompleteTrigger />
                    </InputGroupButton>
                )}
                {showClear && <AutocompleteClear disabled={disabled} />}
            </InputGroupAddon>
            {children}
        </InputGroup>
    );
}

function AutocompleteContent({
    className,
    side = "bottom",
    sideOffset = 6,
    align = "start",
    alignOffset = 0,
    ...props
}: AutocompletePrimitive.Popup.Props &
    Pick<AutocompletePrimitive.Positioner.Props, "side" | "align" | "sideOffset" | "alignOffset">) {
    // `null` tells Base UI to wait for a container; outside a modal we want the
    // default `<body>` portal, so fall back to `undefined`.
    const container = usePortalContainer() ?? undefined;

    return (
        <AutocompletePrimitive.Portal container={container}>
            <AutocompletePrimitive.Positioner
                side={side}
                sideOffset={sideOffset}
                align={align}
                alignOffset={alignOffset}
                className="isolate z-50"
            >
                <AutocompletePrimitive.Popup
                    data-slot="autocomplete-content"
                    className={cn(
                        "group/autocomplete-content relative max-h-(--available-height) w-(--anchor-width) max-w-(--available-width) min-w-(--anchor-width) origin-(--transform-origin) overflow-hidden rounded-lg bg-popover text-popover-foreground shadow-md ring-1 ring-foreground/10 duration-100 data-[side=bottom]:slide-in-from-top-2 data-[side=top]:slide-in-from-bottom-2 data-open:animate-in data-open:fade-in-0 data-open:zoom-in-95 data-closed:animate-out data-closed:fade-out-0 data-closed:zoom-out-95",
                        className,
                    )}
                    {...props}
                />
            </AutocompletePrimitive.Positioner>
        </AutocompletePrimitive.Portal>
    );
}

function AutocompleteList({ className, ...props }: AutocompletePrimitive.List.Props) {
    return (
        <AutocompletePrimitive.List
            data-slot="autocomplete-list"
            className={cn(
                "no-scrollbar max-h-[min(calc(--spacing(72)---spacing(9)),calc(var(--available-height)---spacing(9)))] scroll-py-1 overflow-y-auto overscroll-contain p-1 data-empty:p-0",
                className,
            )}
            {...props}
        />
    );
}

function AutocompleteItem({ className, ...props }: AutocompletePrimitive.Item.Props) {
    return (
        <AutocompletePrimitive.Item
            data-slot="autocomplete-item"
            className={cn(
                "relative flex w-full cursor-default items-center gap-2 rounded-md px-1.5 py-1 text-sm outline-hidden select-none data-disabled:pointer-events-none data-disabled:opacity-50 data-highlighted:bg-accent data-highlighted:text-accent-foreground [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4",
                className,
            )}
            {...props}
        />
    );
}

function AutocompleteEmpty({ className, ...props }: AutocompletePrimitive.Empty.Props) {
    return (
        <AutocompletePrimitive.Empty
            data-slot="autocomplete-empty"
            className={cn(
                "hidden w-full justify-center py-2 text-center text-sm text-muted-foreground group-data-empty/autocomplete-content:flex",
                className,
            )}
            {...props}
        />
    );
}

export {
    Autocomplete,
    AutocompleteInput,
    AutocompleteContent,
    AutocompleteList,
    AutocompleteItem,
    AutocompleteEmpty,
    AutocompleteTrigger,
};
