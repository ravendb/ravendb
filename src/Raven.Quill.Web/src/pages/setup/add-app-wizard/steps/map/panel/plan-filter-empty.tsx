import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";

export function PlanFilterEmpty({ message, onClearFilters }: { message: string; onClearFilters: () => void }) {
    return (
        <div className="flex flex-col items-start gap-2.5 px-0.5 py-1">
            <Text className="text-text-soft">{message}</Text>
            <Button variant="outline" size="sm" onClick={onClearFilters}>
                Clear filters
            </Button>
        </div>
    );
}
