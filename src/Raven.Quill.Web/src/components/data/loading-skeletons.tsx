import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { cn } from "@/lib/utils";

/** Placeholder for a stack of bordered cards, each with a heading and a couple of lines. */
export function CardListSkeleton({ count = 3 }: { count?: number }) {
    return (
        <div className="grid gap-3">
            {Array.from({ length: count }).map((_, index) => (
                <div key={index} className="grid gap-2 rounded-lg border p-4">
                    <Skeleton className="h-4 w-40" />
                    <Skeleton className="h-3 w-full" />
                    <Skeleton className="h-3 w-2/3" />
                </div>
            ))}
        </div>
    );
}

/** Placeholder for a form, matching the label-over-control rhythm of `Field`. */
export function FormFieldsSkeleton({ count = 3 }: { count?: number }) {
    return (
        <div className="grid gap-5">
            {Array.from({ length: count }).map((_, index) => (
                <div key={index} className="grid gap-2">
                    <Skeleton className="h-3.5 w-28" />
                    <Skeleton className="h-8 w-full" />
                </div>
            ))}
        </div>
    );
}

/** Placeholder for a grid of read-only label/value pairs, as used by the detail cards. */
export function DetailGridSkeleton({ count = 4, className }: { count?: number; className?: string }) {
    return (
        <div className={cn("grid gap-6 sm:grid-cols-2", className)}>
            {Array.from({ length: count }).map((_, index) => (
                <div key={index} className="grid gap-1.5">
                    <Skeleton className="h-3 w-20" />
                    <Skeleton className="h-4 w-40" />
                </div>
            ))}
        </div>
    );
}

// Fixed rather than random so the bars do not reshuffle on every render.
const BAR_HEIGHTS = ["h-16", "h-28", "h-20", "h-36", "h-24", "h-44", "h-32", "h-24", "h-40", "h-28", "h-20", "h-36"];

/** Placeholder for a bar chart, matching the `h-56` chart container. */
export function ChartSkeleton() {
    return (
        <div className="flex h-56 w-full items-end gap-2">
            {BAR_HEIGHTS.map((height, index) => (
                <Skeleton key={index} className={cn("flex-1", height)} />
            ))}
        </div>
    );
}
