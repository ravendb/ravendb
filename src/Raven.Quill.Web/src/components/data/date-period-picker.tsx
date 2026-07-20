import { useState } from "react";
import { format } from "date-fns";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Calendar } from "@/components/shadcn/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import {
    canStepDayUp,
    canStepMonthUp,
    canStepYearUp,
    clampToToday,
    formatPeriodLabel,
    stepDay,
    stepMonth,
    stepYear,
    type DatePeriod,
} from "@/lib/date-period";

type Granularity = "year" | "month" | "day";

function getGranularity(period: DatePeriod): Granularity {
    if (period.day !== null) return "day";
    if (period.month !== null) return "month";
    return "year";
}

// Switching to a finer granularity lands on the latest selectable bucket via
// clamping (month 12 / day 31 pulled back to today).
function withGranularity(period: DatePeriod, granularity: Granularity): DatePeriod {
    if (granularity === "year") return { year: period.year, month: null, day: null };
    if (granularity === "month") return clampToToday({ year: period.year, month: period.month ?? 12, day: null });
    return clampToToday({ year: period.year, month: period.month ?? 12, day: period.day ?? 31 });
}

function PickerNav({
    label,
    unit,
    canGoNext,
    onPrev,
    onNext,
}: {
    label: string;
    unit: string;
    canGoNext: boolean;
    onPrev: () => void;
    onNext: () => void;
}) {
    return (
        <div className="flex items-center justify-between">
            <Button variant="ghost" size="icon-sm" aria-label={`Previous ${unit}`} onClick={onPrev}>
                <ChevronLeft aria-hidden="true" />
            </Button>
            <span className="text-sm font-medium">{label}</span>
            <Button variant="ghost" size="icon-sm" aria-label={`Next ${unit}`} disabled={!canGoNext} onClick={onNext}>
                <ChevronRight aria-hidden="true" />
            </Button>
        </div>
    );
}

const YEAR_PAGE_SIZE = 12;

function YearPickerGrid({ value, onSelect }: { value: DatePeriod; onSelect: (period: DatePeriod) => void }) {
    const currentYear = new Date().getFullYear();
    const [pageStart, setPageStart] = useState(Math.floor(value.year / YEAR_PAGE_SIZE) * YEAR_PAGE_SIZE);

    return (
        <div className="flex w-56 flex-col gap-2 p-2">
            <PickerNav
                label={`${pageStart} – ${pageStart + YEAR_PAGE_SIZE - 1}`}
                unit="years"
                canGoNext={pageStart + YEAR_PAGE_SIZE <= currentYear}
                onPrev={() => setPageStart(pageStart - YEAR_PAGE_SIZE)}
                onNext={() => setPageStart(pageStart + YEAR_PAGE_SIZE)}
            />
            <div className="grid grid-cols-3 gap-1">
                {Array.from({ length: YEAR_PAGE_SIZE }, (_, index) => pageStart + index).map((year) => (
                    <Button
                        key={year}
                        variant={year === value.year ? "default" : "ghost"}
                        size="sm"
                        disabled={year > currentYear}
                        onClick={() => onSelect({ year, month: null, day: null })}
                    >
                        {year}
                    </Button>
                ))}
            </div>
        </div>
    );
}

function MonthPickerGrid({ value, onSelect }: { value: DatePeriod; onSelect: (period: DatePeriod) => void }) {
    const now = new Date();
    const [viewYear, setViewYear] = useState(value.year);

    return (
        <div className="flex w-56 flex-col gap-2 p-2">
            <PickerNav
                label={String(viewYear)}
                unit="year"
                canGoNext={viewYear < now.getFullYear()}
                onPrev={() => setViewYear(viewYear - 1)}
                onNext={() => setViewYear(viewYear + 1)}
            />
            <div className="grid grid-cols-3 gap-1">
                {Array.from({ length: 12 }, (_, monthIndex) => (
                    <Button
                        key={monthIndex}
                        variant={viewYear === value.year && monthIndex + 1 === value.month ? "default" : "ghost"}
                        size="sm"
                        disabled={viewYear === now.getFullYear() && monthIndex > now.getMonth()}
                        onClick={() => onSelect({ year: viewYear, month: monthIndex + 1, day: null })}
                    >
                        {format(new Date(viewYear, monthIndex, 1), "MMM")}
                    </Button>
                ))}
            </div>
        </div>
    );
}

function DayPickerCalendar({ value, onSelect }: { value: DatePeriod; onSelect: (period: DatePeriod) => void }) {
    const selected = new Date(value.year, (value.month ?? 1) - 1, value.day ?? 1);

    return (
        <Calendar
            mode="single"
            required
            selected={selected}
            defaultMonth={selected}
            disabled={{ after: new Date() }}
            onSelect={(date) => onSelect({ year: date.getFullYear(), month: date.getMonth() + 1, day: date.getDate() })}
        />
    );
}

// Pages through year/month/day periods one step at a time, capped at today:
// pick the granularity, then one pair of chevrons steps by that unit. Clicking
// the period label opens a picker matching the granularity (years, months, or
// a day calendar).
export function DatePeriodPicker({ value, onChange }: { value: DatePeriod; onChange: (value: DatePeriod) => void }) {
    const granularity = getGranularity(value);
    const [isPickerOpen, setIsPickerOpen] = useState(false);
    const step = granularity === "day" ? stepDay : granularity === "month" ? stepMonth : stepYear;
    const canStepUp =
        granularity === "day"
            ? canStepDayUp(value)
            : granularity === "month"
              ? canStepMonthUp(value)
              : canStepYearUp(value);

    const handlePickerSelect = (period: DatePeriod) => {
        onChange(clampToToday(period));
        setIsPickerOpen(false);
    };

    return (
        <div className="flex w-fit items-center gap-2">
            <ToggleGroup
                type="single"
                variant="outline"
                value={granularity}
                onValueChange={(next) => {
                    if (next !== "") onChange(withGranularity(value, next as Granularity));
                }}
            >
                <ToggleGroupItem value="year">Year</ToggleGroupItem>
                <ToggleGroupItem value="month">Month</ToggleGroupItem>
                <ToggleGroupItem value="day">Day</ToggleGroupItem>
            </ToggleGroup>
            <div className="flex h-8 items-center gap-1 rounded-lg border px-1">
                <Button
                    variant="ghost"
                    size="icon-sm"
                    aria-label={`Previous ${granularity}`}
                    onClick={() => onChange(step(value, -1))}
                >
                    <ChevronLeft aria-hidden="true" />
                </Button>
                <Popover open={isPickerOpen} onOpenChange={setIsPickerOpen}>
                    <PopoverTrigger asChild>
                        <Button variant="ghost" size="sm" className="min-w-32 font-medium">
                            {formatPeriodLabel(value)}
                        </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="center">
                        {granularity === "day" ? (
                            <DayPickerCalendar value={value} onSelect={handlePickerSelect} />
                        ) : granularity === "month" ? (
                            <MonthPickerGrid value={value} onSelect={handlePickerSelect} />
                        ) : (
                            <YearPickerGrid value={value} onSelect={handlePickerSelect} />
                        )}
                    </PopoverContent>
                </Popover>
                <Button
                    variant="ghost"
                    size="icon-sm"
                    aria-label={`Next ${granularity}`}
                    disabled={!canStepUp}
                    onClick={() => onChange(step(value, 1))}
                >
                    <ChevronRight aria-hidden="true" />
                </Button>
            </div>
        </div>
    );
}
