import type { ReactNode } from "react";

export function PlannerMessage({ children }: { children: ReactNode }) {
    return <div className="flex min-w-0 flex-col gap-2.5">{children}</div>;
}
