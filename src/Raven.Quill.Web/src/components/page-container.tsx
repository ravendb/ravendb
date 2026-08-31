import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

// Caps and centres page content so views don't stretch edge-to-edge on wide/2K monitors.
// The `wide` guardrail is applied app-wide and is deliberately generous: it only bites above
// ~1920px, so normal screens are untouched and only big monitors get reined in + centred.
// `narrow`/`default` are for future per-content-type opt-ins (forms, compact detail pages).
const pageContainerVariants = cva("mx-auto w-full", {
    variants: {
        size: {
            narrow: "max-w-[768px]", // forms, focused config, single-record detail
            default: "max-w-[1200px]", // compact lists / detail pages
            wide: "max-w-[1760px]", // app-wide guardrail: only caps screens wider than ~1920
            full: "max-w-none", // dense horizontal content that needs the whole viewport
        },
    },
    defaultVariants: { size: "wide" },
});

export type PageContainerProps = ComponentProps<"div"> & VariantProps<typeof pageContainerVariants>;

export function PageContainer({ size, className, ...props }: PageContainerProps) {
    return <div className={cn(pageContainerVariants({ size }), className)} {...props} />;
}
