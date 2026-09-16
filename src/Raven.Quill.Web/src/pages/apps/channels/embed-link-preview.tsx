import { Copy, ExternalLink } from "lucide-react";
import { Text } from "@/components/typography";
import { CopyableCode } from "@/components/data/copyable-code";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { Timestamp } from "@/components/data/timestamp";
import { copyToClipboard } from "@/lib/utils";

type EmbedLinkPreviewProps = {
    url: string;
    expiresAt: string;
    maxInvocations: number;
};

export function EmbedLinkPreview({ url, expiresAt, maxInvocations }: EmbedLinkPreviewProps) {
    const iframeSnippet = `<iframe src="${url}" width="400" height="600"></iframe>`;

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
                <CopyableCode code={iframeSnippet} copyLabel="Copy embed snippet" />
            </Field>
            <Text variant="caption">
                Expires <Timestamp value={expiresAt} textVariant="inherit" /> · up to {maxInvocations.toLocaleString()}{" "}
                chats.
            </Text>
            <iframe
                src={url}
                title="Embed preview"
                className="mx-auto h-[min(600px,55vh)] w-full max-w-[400px] rounded-lg border bg-white"
            />
        </div>
    );
}
