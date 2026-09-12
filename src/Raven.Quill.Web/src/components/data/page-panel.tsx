import type { ReactNode } from "react";

export function PagePanel({ children }: { children: ReactNode }) {
    return (
        <section className="flex min-h-full min-w-full items-start">
            <div className="w-full pt-4">{children}</div>
        </section>
    );
}
