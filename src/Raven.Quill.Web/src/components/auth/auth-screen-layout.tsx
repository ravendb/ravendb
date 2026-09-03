import type { ReactNode } from "react";
import QuillLogo from "@/components/brand/quill-logo.svg?react";
import { AuthBackground } from "@/components/auth/auth-background";
import { cn } from "@/lib/utils";

const SimpleBackground = () => (
    <div
        aria-hidden
        className="pointer-events-none absolute inset-0"
        style={{
            background:
                "radial-gradient(70% 55% at 50% -10%, color-mix(in oklch, var(--brand-500) 16%, transparent), transparent 70%)",
        }}
    />
);

export function AuthScreenLayout({
    children,
    background = "animated",
}: {
    children: ReactNode;
    background?: "animated" | "simple";
}) {
    // The animated auth screens (login, boot gate, loading) commit to the dark brand look; the simple
    // variant (error boundary) can surface inside the themed app, so it follows the user's theme.
    const isAnimated = background === "animated";

    return (
        <main
            className={cn(
                "relative flex min-h-svh flex-col items-center justify-center overflow-hidden bg-background px-4 py-10 text-foreground",
                isAnimated && "dark",
            )}
        >
            {isAnimated ? <AuthBackground /> : <SimpleBackground />}
            <div className="relative z-10 flex w-full max-w-md flex-col items-center">
                <div className="mb-8 flex items-center gap-2.5 text-foreground">
                    <QuillLogo className="animate-once w-[120px] animate-in duration-600 fade-in-0 zoom-in-90" />
                </div>
                {children}
            </div>
        </main>
    );
}
