import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { cn } from "@/lib/utils";

export function AppAgents() {
    const { slug = "" } = useParams();
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    return (
        <ApiState
            isLoading={agentsQuery.isPending}
            isError={agentsQuery.isError}
            errorTitle="Could not load agents"
            onRetry={agentsQuery.refetch}
            loadingLabel="Loading agents..."
        >
            {agentsQuery.data && (
                <div className="overflow-hidden rounded-lg border">
                    <Table>
                        <TableHeader>
                            <TableRow className="hover:bg-transparent">
                                {["Agent name", "Status", "Model"].map((header) => (
                                    <TableHead key={header} className="text-xs font-medium text-muted-foreground">
                                        {header}
                                    </TableHead>
                                ))}
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {agentsQuery.data.length === 0 ? (
                                <TableRow className="hover:bg-transparent">
                                    <TableCell colSpan={3} className="h-20 text-center text-muted-foreground">
                                        No agents yet.
                                    </TableCell>
                                </TableRow>
                            ) : (
                                agentsQuery.data.map((agent) => (
                                    <TableRow key={agent.agentId}>
                                        <TableCell className="font-medium">{agent.name}</TableCell>
                                        <TableCell>
                                            <AgentStatus isDisabled={agent.disabled} />
                                        </TableCell>
                                        <TableCell className="font-mono text-xs text-muted-foreground">
                                            {agent.model ?? "—"}
                                        </TableCell>
                                    </TableRow>
                                ))
                            )}
                        </TableBody>
                    </Table>
                </div>
            )}
        </ApiState>
    );
}

function AgentStatus({ isDisabled }: { isDisabled: boolean }) {
    return (
        <span
            className={cn(
                "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                isDisabled
                    ? "bg-muted text-muted-foreground"
                    : "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
            )}
        >
            <span
                className={cn("size-1.5 rounded-full", isDisabled ? "bg-muted-foreground/50" : "bg-emerald-500")}
                aria-hidden="true"
            />
            {isDisabled ? "Disabled" : "Active"}
        </span>
    );
}
