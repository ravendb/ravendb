import { Link, useParams } from "react-router";
import { Database, MessageSquareText, Sparkles, SquareKanban } from "lucide-react";
import type { ComponentType } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { DetailList } from "@/components/data/detail-list";
import { PagePanel } from "@/components/data/page-panel";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";

export function AppOverview() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));

    return (
        <PagePanel>
            <ApiState
                isLoading={appQuery.isPending}
                isError={appQuery.isError}
                errorTitle="Could not load app"
                onRetry={() => void appQuery.refetch()}
            >
                {appQuery.data && (
                    <div className="space-y-5">
                        <div className="flex items-center justify-between gap-3">
                            <h2 className="text-sm font-semibold">{appQuery.data.name}</h2>
                            <Button asChild size="sm">
                                <Link to={appRoutes.addCapability(slug)}>
                                    <Sparkles className="size-3.5" aria-hidden="true" />
                                    Add AI Capability
                                </Link>
                            </Button>
                        </div>
                        <DetailList
                            items={[
                                { label: "Name", value: appQuery.data.name },
                                { label: "App id", value: appQuery.data.id },
                                { label: "Database", value: appQuery.data.database },
                                { label: "CDC task", value: appQuery.data.cdcTaskName },
                                { label: "Created", value: formatDate(appQuery.data.createdAt) },
                            ]}
                        />

                        <div className="grid gap-3 md:grid-cols-3">
                            <OverviewLink to="data-source" icon={Database} label="Data source" />
                            <OverviewLink to="tasks" icon={SquareKanban} label="Tasks" />
                            <OverviewLink to="conversations" icon={MessageSquareText} label="Conversations" />
                        </div>
                    </div>
                )}
            </ApiState>
        </PagePanel>
    );
}

function OverviewLink({
    icon: Icon,
    label,
    to,
}: {
    icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
    label: string;
    to: string;
}) {
    return (
        <Link to={to} className="flex items-center gap-3 rounded-md border bg-background p-4 hover:bg-accent">
            <Icon className="size-5" aria-hidden />
            <span className="text-sm font-medium">{label}</span>
        </Link>
    );
}

function formatDate(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
