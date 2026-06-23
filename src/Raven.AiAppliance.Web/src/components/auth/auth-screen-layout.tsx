import type { ReactNode } from "react";
import { RavenLogo } from "@/components/brand/raven-logo";

export function AuthScreenLayout({ children }: { children: ReactNode }) {
    return (
        <main className="relative flex min-h-svh flex-col items-center justify-center overflow-hidden bg-background px-4 py-10">
            <div
                aria-hidden
                className="pointer-events-none absolute inset-0"
                style={{
                    background:
                        "radial-gradient(70% 55% at 50% -10%, color-mix(in oklch, var(--brand-500) 16%, transparent), transparent 70%)",
                }}
            />
            <div className="relative z-10 flex w-full max-w-md flex-col items-center">
                <div className="mb-8 flex items-center gap-2.5 text-foreground">
                    <RavenLogo className="size-9" />
                    <div className="flex flex-col leading-tight">
                        <span className="text-sm font-semibold tracking-tight">RavenDB</span>
                        <span className="text-xs text-muted-foreground">AI Appliance</span>
                    </div>
                </div>
                {children}
            </div>
        </main>
    );
}
