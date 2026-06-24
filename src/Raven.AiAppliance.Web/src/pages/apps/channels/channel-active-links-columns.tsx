/* eslint-disable react-refresh/only-export-components */

import type { ColumnDef } from "@tanstack/react-table";
import { Copy, Eye, Trash2 } from "lucide-react";
import type { EmbedLinkSummaryResponse } from "@/api/generated/server-api";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { copyToClipboard, formatDateTime } from "@/lib/utils";
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
            cell: ({ row }) => <LinkParameters parameters={row.original.parameters} />,
        },
        {
            accessorKey: "createdAt",
            header: "Created",
            cell: ({ row }) => <span className="text-muted-foreground">{formatDateTime(row.original.createdAt)}</span>,
        },
        {
            accessorKey: "expiresAt",
            header: "Expires",
            cell: ({ row }) => <span className="text-muted-foreground">{formatDateTime(row.original.expiresAt)}</span>,
        },
        {
            id: "usage",
            header: "Usage",
            cell: ({ row }) => (
                <span className="text-muted-foreground tabular-nums">
                    {row.original.invocationCount.toLocaleString()} / {row.original.maxInvocations.toLocaleString()}
                </span>
            ),
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

function LinkActions({ slug, link }: { slug: string; link: EmbedLinkSummaryResponse }) {
    return (
        <div className="flex items-center gap-1">
            <Button
                variant="ghost"
                size="icon-sm"
                aria-label={`Copy embed link ${link.token}`}
                title="Copy link"
                onClick={() => copyToClipboard(buildEmbedUrl(link.token))}
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

function LinkParameters({ parameters }: { parameters: Record<string, string> }) {
    const entries = Object.entries(parameters);
    if (entries.length === 0) {
        return <span className="text-muted-foreground">—</span>;
    }

    return (
        <span className="flex gap-1.5">
            {entries.map(([name, value]) => (
                <Badge key={name} variant="secondary" className="font-mono font-normal">
                    {name}: {value}
                </Badge>
            ))}
        </span>
    );
}
