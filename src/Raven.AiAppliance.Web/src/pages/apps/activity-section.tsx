import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatDateTime } from "@/lib/utils";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function ActivitySection({ slug }: { slug: string }) {
    const activityQuery = useQuery(api.queries.stats.activity(slug));

    return (
        <SectionCard title="Recent activity">
            <ApiState
                isLoading={activityQuery.isPending}
                isError={activityQuery.isError}
                errorTitle="Could not load activity"
                onRetry={() => void activityQuery.refetch()}
                loadingLabel="Loading activity..."
            >
                {activityQuery.data && (
                    <SectionTable
                        headers={["Event", "Time"]}
                        isEmpty={activityQuery.data.length === 0}
                        emptyMessage="No recent activity yet."
                    >
                        {activityQuery.data.map((event) => (
                            <TableRow key={event.id}>
                                <TableCell>
                                    <div className="flex flex-col gap-0.5">
                                        <span className="font-medium">{event.message}</span>
                                        {event.type && (
                                            <span className="text-xs text-muted-foreground">{event.type}</span>
                                        )}
                                    </div>
                                </TableCell>
                                <TableCell className="whitespace-nowrap text-muted-foreground">
                                    {formatDateTime(event.timestamp)}
                                </TableCell>
                            </TableRow>
                        ))}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
    );
}
