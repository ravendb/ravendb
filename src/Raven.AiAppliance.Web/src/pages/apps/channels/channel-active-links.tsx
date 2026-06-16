import { useQuery } from "@tanstack/react-query";
import { Copy, Eye, Trash2 } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatDateTime, copyToClipboard } from "@/lib/utils";
import { SectionTable } from "@/pages/apps/section-card";
import { buildEmbedUrl } from "@/pages/apps/channels/embed-link-utils";
import { PreviewEmbedLinkDialog } from "@/pages/apps/channels/preview-embed-link-dialog";
import { RevokeEmbedLinkDialog } from "@/pages/apps/channels/revoke-embed-link-dialog";

export function ChannelActiveLinks({ slug, widgetId }: { slug: string; widgetId: string }) {
    const linksQuery = useQuery(api.queries.embedLinks.list(slug));
    const links = (linksQuery.data ?? []).filter((link) => link.widgetId === widgetId);

    return (
        <ApiState
            isLoading={linksQuery.isPending}
            isError={linksQuery.isError}
            errorTitle="Could not load links"
            onRetry={() => void linksQuery.refetch()}
            loadingLabel="Loading links..."
        >
            <SectionTable
                headers={["Token", "Parameters", "Created", "Expires", "Usage", ""]}
                isEmpty={links.length === 0}
                emptyMessage="No active links. Generate one to embed this agent for a specific user."
            >
                {links.map((link) => (
                    <TableRow key={link.token}>
                        <TableCell className="font-mono text-xs" title={link.token}>
                            {link.token.slice(0, 8)}…
                        </TableCell>
                        <TableCell>
                            <LinkParameters parameters={link.parameters} />
                        </TableCell>
                        <TableCell className="text-muted-foreground">{formatDateTime(link.createdAt)}</TableCell>
                        <TableCell className="text-muted-foreground">{formatDateTime(link.expiresAt)}</TableCell>
                        <TableCell className="text-muted-foreground tabular-nums">
                            {link.invocationCount.toLocaleString()} / {link.maxInvocations.toLocaleString()}
                        </TableCell>
                        <TableCell className="text-right">
                            <div className="flex items-center justify-end gap-1">
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
                                        <Button
                                            variant="ghost"
                                            size="icon-sm"
                                            aria-label={`Revoke link ${link.token}`}
                                            title="Revoke link"
                                        >
                                            <Trash2 className="size-3.5" aria-hidden="true" />
                                        </Button>
                                    }
                                />
                            </div>
                        </TableCell>
                    </TableRow>
                ))}
            </SectionTable>
        </ApiState>
    );
}

function LinkParameters({ parameters }: { parameters: Record<string, string> }) {
    const entries = Object.entries(parameters);
    if (entries.length === 0) {
        return <span className="text-muted-foreground">—</span>;
    }

    return (
        <div className="flex flex-wrap gap-1.5">
            {entries.map(([name, value]) => (
                <Badge key={name} variant="secondary" className="font-mono font-normal">
                    {name}: {value}
                </Badge>
            ))}
        </div>
    );
}
