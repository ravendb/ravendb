import type { ReactNode } from "react";

export function PagePanel({ children }: { children: ReactNode }) {
    return (
        <div className="flex min-h-full w-full items-start">
            <section className="w-full rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
                {children}
            </section>
        </div>
    );
}
