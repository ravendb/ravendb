import type { ComponentProps } from "react";
import { CircleAlert, Loader2Icon, TriangleAlert } from "lucide-react";

import { Badge } from "@/components/shadcn/ui/badge";

export type StatusTone = "positive" | "muted" | "warning" | "info" | "danger" | "loading";

const TONE_VARIANTS: Record<StatusTone, ComponentProps<typeof Badge>["variant"]> = {
    positive: "success",
    muted: "secondary",
    info: "info",
    loading: "secondary",
    warning: "warning",
    danger: "destructive",
};

// Only the tones that ask something of the operator carry an icon, so an icon always reads as
// "this needs you" rather than decoration. The calm tones are label-only. The icons are hidden
// from assistive tech because the badge label already names the state.
function ToneIcon({ tone }: { tone: StatusTone }) {
    switch (tone) {
        case "warning":
            return <TriangleAlert aria-hidden="true" />;
        case "danger":
            return <CircleAlert aria-hidden="true" />;
        case "loading":
            return <Loader2Icon className="animate-spin" aria-hidden="true" />;
        default:
            return null;
    }
}

// The single status style for the whole app.
export function StatusIndicator({
    tone,
    label,
    title,
    className,
}: {
    tone: StatusTone;
    label: string;
    title?: string;
    className?: string;
}) {
    return (
        <Badge variant={TONE_VARIANTS[tone]} className={className} title={title}>
            <ToneIcon tone={tone} />
            {label}
        </Badge>
    );
}

// Channels and agents share one on/off state, so the wording lives here only — otherwise it
// drifts between the channel cards, the tables, the detail header and the capability wizard.
export function EnabledStatus({ isEnabled }: { isEnabled: boolean }) {
    return <StatusIndicator tone={isEnabled ? "positive" : "muted"} label={isEnabled ? "Active" : "Disabled"} />;
}
