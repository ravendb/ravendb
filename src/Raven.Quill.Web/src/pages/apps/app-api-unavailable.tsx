import { PagePanel } from "@/components/data/page-panel";

export function AppApiUnavailable({ feature }: { feature: string }) {
    return (
        <PagePanel>
            <div className="max-w-2xl space-y-2">
                <h2 className="text-base font-semibold">{feature}</h2>
                <p className="text-sm text-muted-foreground">Coming soon.</p>
            </div>
        </PagePanel>
    );
}
