import { type ComponentProps } from "react";
import { Badge } from "@/components/shadcn/ui/badge";
import type { SyncStatus } from "@/pages/apps/sync-status";

const SYNC_STATUS_BADGES: Record<SyncStatus, { variant: ComponentProps<typeof Badge>["variant"]; label: string }> = {
    active: { variant: "success", label: "Active" },
    idle: { variant: "secondary", label: "Idle" },
    error: { variant: "destructive", label: "Error" },
    disabled: { variant: "outline", label: "Disabled" },
};

export function SyncStatusBadge({ status }: { status: SyncStatus }) {
    const badge = SYNC_STATUS_BADGES[status];
    return <Badge variant={badge.variant}>{badge.label}</Badge>;
}
