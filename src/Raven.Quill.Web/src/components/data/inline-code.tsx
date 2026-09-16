import type { PropsWithChildren } from "react";
import { cn } from "@/lib/utils";

export function InlineCode({ className, children }: PropsWithChildren<{ className?: string }>) {
    return <code className={cn("rounded bg-muted px-1 py-0.5 font-mono", className)}>{children}</code>;
}
