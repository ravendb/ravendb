import type { ReactNode } from "react";

export type DetailListItem = {
    label: ReactNode;
    value: ReactNode;
};

export function DetailList({ items }: { items: DetailListItem[] }) {
    return (
        <dl className="grid gap-3 text-sm sm:grid-cols-2 xl:grid-cols-3">
            {items.map((item, index) => (
                <div key={index} className="rounded-md border bg-background p-3">
                    <dt className="text-xs font-medium text-muted-foreground">{item.label}</dt>
                    <dd className="mt-1 min-w-0 font-medium break-words">{item.value || "-"}</dd>
                </div>
            ))}
        </dl>
    );
}
