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
    clampPeriod,
    formatPeriodLabel,
    isSameDatePeriod,
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
function withGranularity(period: DatePeriod, granularity: Granularity, earliest: Date | undefined): DatePeriod {
    if (granularity === "year") return clampPeriod({ year: period.year, month: null, day: null }, earliest);
    if (granularity === "month")
        return clampPeriod({ year: period.year, month: period.month ?? 12, day: null }, earliest);
    return clampPeriod({ year: period.year, month: period.month ?? 12, day: period.day ?? 31 }, earliest);
}

function PickerNav({
    label,
    unit,
    canGoPrev,
    canGoNext,
    onPrev,
    onNext,
}: {
    label: string;
    unit: string;
    canGoPrev: boolean;
    canGoNext: boolean;
    onPrev: () => void;
    onNext: () => void;
}) {
    return (
        <div className="flex items-center justify-between">
            <Button
                variant="ghost"
                size="icon-sm"
                aria-label={`Previous ${unit}`}
                disabled={!canGoPrev}
                onClick={onPrev}
            >
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

type PickerGridProps = {
    value: DatePeriod;
    earliest: Date | undefined;
    onSelect: (period: DatePeriod) => void;
};

function YearPickerGrid({ value, earliest, onSelect }: PickerGridProps) {
    const currentYear = new Date().getFullYear();
    const earliestYear = earliest?.getFullYear();
    const [pageStart, setPageStart] = useState(Math.floor(value.year / YEAR_PAGE_SIZE) * YEAR_PAGE_SIZE);

    return (
        <div className="flex w-56 flex-col gap-2 p-2">
            <PickerNav
                label={`${pageStart} – ${pageStart + YEAR_PAGE_SIZE - 1}`}
                unit="years"
                canGoPrev={earliestYear === undefined || pageStart > earliestYear}
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
                        disabled={year > currentYear || (earliestYear !== undefined && year < earliestYear)}
                        onClick={() => onSelect({ year, month: null, day: null })}
                    >
                        {year}
                    </Button>
                ))}
            </div>
        </div>
    );
}

function MonthPickerGrid({ value, earliest, onSelect }: PickerGridProps) {
    const now = new Date();
    const [viewYear, setViewYear] = useState(value.year);

    const isBeforeSetup = (monthIndex: number) =>
        earliest !== undefined &&
        (viewYear < earliest.getFullYear() ||
            (viewYear === earliest.getFullYear() && monthIndex < earliest.getMonth()));

    return (
        <div className="flex w-56 flex-col gap-2 p-2">
            <PickerNav
                label={String(viewYear)}
                unit="year"
                canGoPrev={earliest === undefined || viewYear > earliest.getFullYear()}
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
                        disabled={
                            (viewYear === now.getFullYear() && monthIndex > now.getMonth()) || isBeforeSetup(monthIndex)
                        }
                        onClick={() => onSelect({ year: viewYear, month: monthIndex + 1, day: null })}
                    >
                        {format(new Date(viewYear, monthIndex, 1), "MMM")}
                    </Button>
                ))}
            </div>
        </div>
    );
}

function DayPickerCalendar({ value, earliest, onSelect }: PickerGridProps) {
    const selected = new Date(value.year, (value.month ?? 1) - 1, value.day ?? 1);

    return (
        <Calendar
            mode="single"
            required
            selected={selected}
            defaultMonth={selected}
            startMonth={earliest}
            // Two matchers rather than one { before, after }, which day-picker reads as the
            // interval between the two dates instead of everything outside it.
            disabled={earliest ? [{ before: earliest }, { after: new Date() }] : { after: new Date() }}
            onSelect={(date) => onSelect({ year: date.getFullYear(), month: date.getMonth() + 1, day: date.getDate() })}
        />
    );
}

// Pages through year/month/day periods one step at a time, bounded by `earliest` and
// today: pick the granularity, then one pair of chevrons steps by that unit. Clicking the
// period label opens a picker matching the granularity (years, months, or a day calendar).
// Callers own the lower bound, because it differs per view: useSetupStartDate() for
// server-wide views, useAppStartDate(slug) for a single app's.
export function DatePeriodPicker({
    value,
    earliest,
    onChange,
}: {
    value: DatePeriod;
    /** Earliest selectable date; undefined leaves the picker capped only at today. */
    earliest: Date | undefined;
    onChange: (value: DatePeriod) => void;
}) {
    const granularity = getGranularity(value);
    const [isPickerOpen, setIsPickerOpen] = useState(false);
    const step = granularity === "day" ? stepDay : granularity === "month" ? stepMonth : stepYear;
    const canStepUp =
        granularity === "day"
            ? canStepDayUp(value)
            : granularity === "month"
              ? canStepMonthUp(value)
              : canStepYearUp(value);
    // Stepping down clamps back onto the same period once the bound is reached, which is
    // exactly when the chevron has nothing left to do.
    const canStepDown = !isSameDatePeriod(step(value, -1, earliest), value);

    const handlePickerSelect = (period: DatePeriod) => {
        onChange(clampPeriod(period, earliest));
        setIsPickerOpen(false);
    };

    return (
        <div className="flex w-fit items-center gap-2">
            <ToggleGroup
                type="single"
                variant="outline"
                value={granularity}
                onValueChange={(next) => {
                    if (next !== "") onChange(withGranularity(value, next as Granularity, earliest));
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
                    disabled={!canStepDown}
                    onClick={() => onChange(step(value, -1, earliest))}
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
                            <DayPickerCalendar value={value} earliest={earliest} onSelect={handlePickerSelect} />
                        ) : granularity === "month" ? (
                            <MonthPickerGrid value={value} earliest={earliest} onSelect={handlePickerSelect} />
                        ) : (
                            <YearPickerGrid value={value} earliest={earliest} onSelect={handlePickerSelect} />
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
