/* eslint-disable react-refresh/only-export-components */

import type { ReactNode } from "react";
import type { ColumnDef } from "@tanstack/react-table";
import { CircleAlert, Copy, Eye, Trash2, TriangleAlert } from "lucide-react";
import type { EmbedLinkSummaryResponse } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { copyToClipboard, formatDateTime } from "@/lib/utils";
import { type EmbedLinkStatusTone, getExpiryStatus, getUsageStatus } from "@/pages/apps/channels/embed-link-status";
import { buildEmbedUrl } from "@/pages/apps/channels/embed-link-utils";
import { PreviewEmbedLinkDialog } from "@/pages/apps/channels/preview-embed-link-dialog";
import { RevokeEmbedLinkDialog } from "@/pages/apps/channels/revoke-embed-link-dialog";

export function createActiveLinkColumns(slug: string): ColumnDef<EmbedLinkSummaryResponse>[] {
    return [
        {
            accessorKey: "token",
            header: "Token",
            cell: ({ getValue }) => (
                <span className="font-mono text-xs" title={getValue<string>()}>
                    {getValue<string>()}
                </span>
            ),
        },
        {
            id: "parameters",
            header: "Parameters",
            cell: ({ row }) => (
                <Parameters
                    params={Object.entries(row.original.parameters).map(([name, value]) => ({ name, value }))}
                />
            ),
        },
        {
            accessorKey: "createdAt",
            header: "Created",
            cell: ({ row }) => <span className="text-muted-foreground">{formatDateTime(row.original.createdAt)}</span>,
        },
        {
            accessorKey: "expiresAt",
            header: "Expires",
            cell: ({ row }) => {
                const status = getExpiryStatus(row.original.expiresAt);
                return (
                    <StatusValue tone={status.tone} title={status.title}>
                        {status.label}
                    </StatusValue>
                );
            },
        },
        {
            id: "usage",
            header: "Usage",
            cell: ({ row }) => {
                const status = getUsageStatus(row.original.invocationCount, row.original.maxInvocations);
                return (
                    <StatusValue tone={status.tone} title={status.title}>
                        {status.label}
                    </StatusValue>
                );
            },
        },
        {
            id: "actions",
            header: "",
            size: 120,
            enableResizing: false,
            cell: ({ row }) => <LinkActions slug={slug} link={row.original} />,
        },
    ];
}

// Renders an expiry/usage value, escalating to a colored badge with an icon once the
// link needs attention so problem rows stand out at a glance.
function StatusValue({ tone, title, children }: { tone: EmbedLinkStatusTone; title: string; children: ReactNode }) {
    if (tone === "normal") {
        return (
            <span className="text-muted-foreground tabular-nums" title={title}>
                {children}
            </span>
        );
    }

    const Icon = tone === "critical" ? CircleAlert : TriangleAlert;
    return (
        <Badge variant={tone === "critical" ? "destructive" : "warning"} className="tabular-nums" title={title}>
            <Icon aria-hidden="true" />
            {children}
        </Badge>
    );
}

function LinkActions({ slug, link }: { slug: string; link: EmbedLinkSummaryResponse }) {
    return (
        <div className="flex items-center gap-1">
            <Button
                variant="ghost"
                size="icon-sm"
                aria-label={`Copy embed link ${link.token}`}
                title="Copy link"
                onClick={() => copyToClipboard(buildEmbedUrl(slug, link.token))}
            >
                <Copy className="size-3.5" aria-hidden="true" />
            </Button>
            <PreviewEmbedLinkDialog
                slug={slug}
                link={link}
                trigger={
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        aria-label={`Preview embed link ${link.token}`}
                        title="Preview link"
                    >
                        <Eye className="size-3.5" aria-hidden="true" />
                    </Button>
                }
            />
            <RevokeEmbedLinkDialog
                slug={slug}
                token={link.token}
                trigger={
                    <Button variant="ghost" size="icon-sm" aria-label={`Revoke link ${link.token}`} title="Revoke link">
                        <Trash2 className="size-3.5" aria-hidden="true" />
                    </Button>
                }
            />
        </div>
    );
}
