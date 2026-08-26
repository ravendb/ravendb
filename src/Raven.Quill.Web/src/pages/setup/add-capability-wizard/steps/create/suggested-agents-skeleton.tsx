import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { cn } from "@/lib/utils";

export function SuggestedAgentCardsSkeleton({ isDisabled }: { isDisabled?: boolean }) {
    const lineClasses = isDisabled && "animate-none";

    return (
        <div className={cn("grid auto-cols-[minmax(0,1fr)] grid-flow-col gap-3", isDisabled && "opacity-55")}>
            {Array.from({ length: 3 }).map((_, index) => (
                <div key={index} className="grid min-h-28 content-start gap-2 rounded-lg border p-4">
                    <Skeleton className={cn("h-4 w-2/3", lineClasses)} />
                    <Skeleton className={cn("h-3 w-full", lineClasses)} />
                    <Skeleton className={cn("h-3 w-11/12", lineClasses)} />
                    <Skeleton className={cn("h-3 w-3/4", lineClasses)} />
                </div>
            ))}
        </div>
    );
}
