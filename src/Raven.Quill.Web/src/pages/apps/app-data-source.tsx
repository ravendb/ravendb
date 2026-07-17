import type { ComponentType, ReactNode } from "react";
import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { AppWindow, CalendarClock, Database } from "lucide-react";
import { api } from "@/api/api";
import type { AppResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { PagePanel } from "@/components/data/page-panel";
import { Card, CardContent } from "@/components/shadcn/ui/card";
import { CdcPerformanceSection } from "@/pages/apps/cdc-performance-section";
import { CollectionsSection } from "@/pages/apps/collections-section";
import { formatDate } from "@/lib/format";
import { SectionCard } from "@/pages/apps/section-card";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
            <div className="space-y-8">
                <SectionCard title="Connection">
                    <ApiState
                        isLoading={appQuery.isPending}
                        onRetry={appQuery.refetch}
                        isError={appQuery.isError}
                        errorTitle="Could not load data source"
                    >
                        {appQuery.data && <ConnectionCard app={appQuery.data} />}
                    </ApiState>
                </SectionCard>
                <CdcPerformanceSection slug={slug} />
                <CollectionsSection slug={slug} />
            </div>
        </PagePanel>
    );
}

function ConnectionCard({ app }: { app: AppResponse }) {
    return (
        <Card>
            <CardContent className="grid gap-6 sm:grid-cols-3">
                <ConnectionDetail icon={AppWindow} label="Application" value={app.name} />
                <ConnectionDetail
                    icon={Database}
                    label="Source database"
                    value={<span className="font-mono">{app.database}</span>}
                />
                <ConnectionDetail icon={CalendarClock} label="Connected since" value={formatDate(app.createdAt)} />
            </CardContent>
        </Card>
    );
}

function ConnectionDetail({
    icon: Icon,
    label,
    value,
}: {
    icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
    label: string;
    value: ReactNode;
}) {
    return (
        <div className="flex items-center gap-3">
            <div className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-muted">
                <Icon className="size-4 text-muted-foreground" aria-hidden={true} />
            </div>
            <div className="min-w-0 space-y-0.5">
                <div className="text-xs text-muted-foreground">{label}</div>
                <div className="truncate text-sm font-medium">{value}</div>
            </div>
        </div>
    );
}
