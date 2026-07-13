import type { PropsWithChildren } from "react";

export function InlineCode({ children }: PropsWithChildren) {
    return <code className="rounded bg-muted px-1 py-0.5 font-mono">{children}</code>;
}
