import { useState } from "react";
import { format, isSameDay, startOfToday, subDays } from "date-fns";
import { CalendarIcon, ChevronsUpDownIcon } from "lucide-react";
import type { DateRange } from "react-day-picker";
import { Button } from "@/components/shadcn/ui/button";
import { Calendar } from "@/components/shadcn/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { cn } from "@/lib/utils";

type Preset = { label: string; getRange: () => DateRange };

const PRESETS: Preset[] = [
    { label: "Last 7 days", getRange: () => ({ from: subDays(startOfToday(), 6), to: startOfToday() }) },
    { label: "Last 14 days", getRange: () => ({ from: subDays(startOfToday(), 13), to: startOfToday() }) },
    { label: "Last 30 days", getRange: () => ({ from: subDays(startOfToday(), 29), to: startOfToday() }) },
];

function isSameRange(a: DateRange | undefined, b: DateRange) {
    return Boolean(a?.from && a.to && b.from && b.to && isSameDay(a.from, b.from) && isSameDay(a.to, b.to));
}

function formatTrigger(range: DateRange | undefined) {
    if (!range?.from) {
        return "Pick a date range";
    }
    if (!range.to) {
        return format(range.from, "MM/dd/yy");
    }
    return `${format(range.from, "MM/dd/yy")} - ${format(range.to, "MM/dd/yy")}`;
}

export function DateRangePicker({
    value,
    onChange,
    className,
    align = "end",
}: {
    value: DateRange | undefined;
    onChange: (range: DateRange | undefined) => void;
    className?: string;
    align?: "start" | "center" | "end";
}) {
    const [open, setOpen] = useState(false);
    const today = startOfToday();

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button variant="outline" className={cn("w-60 justify-between font-normal", className)}>
                    <span className="flex items-center gap-2">
                        <CalendarIcon className="text-muted-foreground" />
                        {formatTrigger(value)}
                    </span>
                    <ChevronsUpDownIcon className="text-muted-foreground" />
                </Button>
            </PopoverTrigger>
            <PopoverContent className="flex w-auto flex-row gap-0 overflow-hidden p-0" align={align}>
                <div className="flex w-40 flex-col gap-0.5 border-r p-2">
                    {PRESETS.map((preset) => (
                        <Button
                            key={preset.label}
                            variant={isSameRange(value, preset.getRange()) ? "secondary" : "ghost"}
                            size="sm"
                            className="w-full justify-start font-normal"
                            onClick={() => onChange(preset.getRange())}
                        >
                            {preset.label}
                        </Button>
                    ))}
                </div>
                <Calendar
                    mode="range"
                    numberOfMonths={1}
                    defaultMonth={value?.to ?? value?.from}
                    selected={value}
                    onSelect={onChange}
                    disabled={{ after: today }}
                    autoFocus
                />
            </PopoverContent>
        </Popover>
    );
}
