import type { ReactNode } from "react";
import { PageContainer } from "@/components/page-container";
import { Heading } from "@/components/typography";
import { cn } from "@/lib/utils";

type AppShellMainProps = {
    // Omit for pages that render their own heading.
    title?: string;
    // Full-screen flows like the setup wizards: no padding and no width cap.
    isBareLayout?: boolean;
    className?: string;
    children: ReactNode;
};

// The routed page area of the app shell. Storybook renders pages in it too, so a story looks like the app.
export function AppShellMain({ title, isBareLayout = false, className, children }: AppShellMainProps) {
    return (
        <main
            className={cn(
                "app-shell__main rounded-lg border bg-surface1 dark:bg-surface2",
                isBareLayout ? "gap-0 p-0" : "gap-3 px-4 py-5 lg:px-5",
                !title && "grid-rows-[minmax(0,1fr)]",
                className,
            )}
        >
            {title && (
                // Mirror the scroll area below (same bleed + scrollbar-gutter) so the centred title lines up
                // with the centred content whether or not the content shows a scrollbar. `overflow-hidden`
                // makes this a scroll container so `scrollbar-gutter` reserves the same space here too.
                <div className="-mx-4 [scrollbar-gutter:stable] overflow-hidden px-4 lg:-mx-5 lg:px-5">
                    <PageContainer>
                        <Heading as="h1" variant="page">
                            {title}
                        </Heading>
                    </PageContainer>
                </div>
            )}
            <div
                className={cn(
                    "min-h-0",
                    // Bleed the scroll area to the panel's inner border (padding keeps content in place) so
                    // a full-bleed detail header can reach the edges instead of being clipped short. Clip
                    // horizontal overflow so the page never scrolls sideways; wide content self-scrolls.
                    // `scrollbar-gutter: stable` reserves the scrollbar space even when no scrollbar shows; the
                    // title band above mirrors this exact box, so the two centre on the same axis either way.
                    isBareLayout
                        ? "overflow-auto"
                        : "-mx-4 [scrollbar-gutter:stable] overflow-x-clip overflow-y-auto px-4 lg:-mx-5 lg:px-5",
                )}
            >
                {/* Bare layouts (wizards) manage their own width; every other page is capped and
                    centred here so content matches the title band instead of stretching on wide screens. */}
                {isBareLayout ? children : <PageContainer>{children}</PageContainer>}
            </div>
        </main>
    );
}
