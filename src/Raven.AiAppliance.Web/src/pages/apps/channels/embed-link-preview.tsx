import { Copy, ExternalLink } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { formatDateTime, copyToClipboard } from "@/lib/utils";

type EmbedLinkPreviewProps = {
    /** Absolute, paste-ready embed URL for the customer's cross-origin <iframe src>. */
    url: string;
    /** The opaque bearer token — drives the relative inline preview. */
    token: string;
    expiresAt: string;
    maxInvocations: number;
};

/**
 * The minted-link result view shared by the "Generate embed link" dialog and the
 * per-link preview dialog: the paste-ready URL (copy + open), the iframe snippet,
 * the TTL/cap line, and an inline live preview. Renders its own grid container, so
 * place a sibling footer next to it inside a `DialogContent`.
 */
export function EmbedLinkPreview({ url, token, expiresAt, maxInvocations }: EmbedLinkPreviewProps) {
    const iframeSnippet = `<iframe src="${url}" width="400" height="600"></iframe>`;
    // The url is absolute (paste-ready for the customer). The inline preview loads the
    // token relatively so it works behind the dev /embed proxy and same-origin in
    // production, regardless of the absolute host the URL points at.
    const previewUrl = `/embed/${token}`;

    return (
        <div className="grid min-w-0 gap-4">
            <Field>
                <FieldLabel>Embed URL</FieldLabel>
                <InputGroup>
                    <InputGroupInput readOnly value={url} className="font-mono text-xs" />
                    <InputGroupAddon align="inline-end">
                        <InputGroupButton size="icon-xs" aria-label="Open link in a new tab" asChild>
                            <a href={url} target="_blank" rel="noreferrer">
                                <ExternalLink />
                            </a>
                        </InputGroupButton>
                        <InputGroupButton
                            size="icon-xs"
                            aria-label="Copy embed URL"
                            onClick={() => copyToClipboard(url)}
                        >
                            <Copy />
                        </InputGroupButton>
                    </InputGroupAddon>
                </InputGroup>
            </Field>

            <Field>
                <FieldLabel>Embed snippet</FieldLabel>
                <div className="relative min-w-0">
                    <pre className="rounded-lg border bg-muted/50 py-2 pr-10 pl-3 text-xs break-all whitespace-pre-wrap">
                        <code>{iframeSnippet}</code>
                    </pre>
                    <Button
                        type="button"
                        variant="ghost"
                        size="icon-sm"
                        className="absolute top-1.5 right-1.5"
                        aria-label="Copy embed snippet"
                        onClick={() => copyToClipboard(iframeSnippet)}
                    >
                        <Copy className="size-3.5" aria-hidden="true" />
                    </Button>
                </div>
            </Field>

            <p className="text-xs text-muted-foreground">
                Expires {formatDateTime(expiresAt)} · up to {maxInvocations.toLocaleString()} chats.
            </p>

            {/* Rendered at the widget's real 400px width (matching the snippet) so the preview
                looks exactly as embedded. The embed page styles itself light-only, so the
                backdrop stays white in dark mode too.
                KNOWN LIMITATION (RavenDB-26775): this previews the *real* minted token, so
                sending a message here spends one of the link's invocations and pins its
                single server-owned conversation — the end user then inherits that history and
                a reduced budget. Merely opening the preview is harmless (only POST /chat
                counts). A dedicated un-counted preview surface is deferred. */}
            <iframe
                src={previewUrl}
                title="Embed preview"
                className="mx-auto h-[min(600px,55vh)] w-full max-w-[400px] rounded-lg border bg-white"
            />
        </div>
    );
}
