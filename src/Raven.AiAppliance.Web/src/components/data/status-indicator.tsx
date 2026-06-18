import { CircleCheckIcon } from "lucide-react";

import { Badge } from "@/components/shadcn/ui/badge";

export function StatusIndicator({ tone, label }: { tone: "positive" | "muted"; label: string }) {
    const isPositive = tone === "positive";

    return (
        <Badge variant={isPositive ? "success" : "secondary"}>
            {isPositive && <CircleCheckIcon aria-hidden="true" />}
            {label}
        </Badge>
    );
}
