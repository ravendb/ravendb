import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { formatMonthLabel, getCurrentMonth, type MonthSelection } from "@/lib/month";

// Steps through calendar months one at a time, capped at the current month.
export function MonthPicker({ value, onChange }: { value: MonthSelection; onChange: (value: MonthSelection) => void }) {
    const current = getCurrentMonth();
    const isAtCurrentMonth = value.year === current.year && value.month === current.month;

    const goToMonth = (offset: number) => {
        const next = new Date(value.year, value.month - 1 + offset);
        onChange({ year: next.getFullYear(), month: next.getMonth() + 1 });
    };

    return (
        <div className="flex items-center gap-1 rounded-lg border p-1">
            <Button variant="ghost" size="icon-sm" aria-label="Previous month" onClick={() => goToMonth(-1)}>
                <ChevronLeft aria-hidden="true" />
            </Button>
            <span className="min-w-28 text-center text-sm font-medium">{formatMonthLabel(value)}</span>
            <Button
                variant="ghost"
                size="icon-sm"
                aria-label="Next month"
                disabled={isAtCurrentMonth}
                onClick={() => goToMonth(1)}
            >
                <ChevronRight aria-hidden="true" />
            </Button>
        </div>
    );
}
