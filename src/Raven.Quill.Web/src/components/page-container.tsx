import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

// Caps and centres page content so views don't stretch edge-to-edge on wide/2K monitors.
// The guardrail is applied app-wide and is deliberately generous: it only bites above
// ~1920px, so normal screens are untouched and only big monitors get reined in + centred.
export function PageContainer({ className, ...props }: ComponentProps<"div">) {
    return <div className={cn("mx-auto w-full max-w-[1760px]", className)} {...props} />;
}
