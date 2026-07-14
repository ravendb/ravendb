import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
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

// Pages through year/month/day periods one step at a time, capped at today:
// pick the granularity, then one pair of chevrons steps by that unit.
export function DatePeriodPicker({ value, onChange }: { value: DatePeriod; onChange: (value: DatePeriod) => void }) {
    const granularity = getGranularity(value);
    const step = granularity === "day" ? stepDay : granularity === "month" ? stepMonth : stepYear;
    const canStepUp =
        granularity === "day"
            ? canStepDayUp(value)
            : granularity === "month"
              ? canStepMonthUp(value)
              : canStepYearUp(value);

    return (
        <div className="flex w-fit items-center gap-2">
            <ToggleGroup
                type="single"
                variant="outline"
                size="sm"
                value={granularity}
                onValueChange={(next) => {
                    if (next !== "") onChange(withGranularity(value, next as Granularity));
                }}
            >
                <ToggleGroupItem value="year">Year</ToggleGroupItem>
                <ToggleGroupItem value="month">Month</ToggleGroupItem>
                <ToggleGroupItem value="day">Day</ToggleGroupItem>
            </ToggleGroup>
            <div className="flex items-center gap-1 rounded-lg border p-1">
                <Button
                    variant="ghost"
                    size="icon-sm"
                    aria-label={`Previous ${granularity}`}
                    onClick={() => onChange(step(value, -1))}
                >
                    <ChevronLeft aria-hidden="true" />
                </Button>
                <span className="min-w-32 text-center text-sm font-medium">{formatPeriodLabel(value)}</span>
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
