import type { ComponentProps } from "react";
import { Badge } from "@/components/shadcn/ui/badge";
import { cn } from "@/lib/utils";

// The one badge for numeric counts (items in a section, rows in a table). Keeps every count looking
// the same — secondary tone, tabular figures so digits don't jitter as the number changes.
export function CountBadge({ className, ...props }: ComponentProps<typeof Badge>) {
    return <Badge variant="secondary" className={cn("tabular-nums", className)} {...props} />;
}
