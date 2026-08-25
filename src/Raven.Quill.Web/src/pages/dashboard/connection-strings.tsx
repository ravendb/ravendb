import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { TableSkeletonRows } from "@/components/table/table-skeleton";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import { EditAiConnectionString } from "@/components/ai-connection-string/edit-ai-connection-string";
import { DeleteAiConnectionStringDialog } from "@/components/ai-connection-string/delete-ai-connection-string-dialog";
import { getProviderLabel, MODEL_TYPE_LABELS } from "@/components/ai-connection-string/ai-connection-string-utils";

const CONNECTION_STRINGS_COLUMN_COUNT = 4;

export function DashboardConnectionStrings() {
    const connectionStringsQuery = useQuery(api.queries.aiConnectionStrings.list());
    const items = connectionStringsQuery.data ?? [];

    const refetch = () => void connectionStringsQuery.refetch();

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                    <h1 className="text-2xl font-semibold tracking-tight">AI connection strings</h1>
                    <p className="text-sm text-muted-foreground">
                        Provider credentials agents use to reach a model, shared by every app on this server.
                    </p>
                </div>
                <AddAiConnectionString
                    modelType="Chat"
                    onCreated={refetch}
                    trigger={
                        <Button size="sm">
                            <Plus className="size-3.5" aria-hidden="true" />
                            Add connection string
                        </Button>
                    }
                />
            </div>

            <ApiState
                isLoading={connectionStringsQuery.isPending}
                isError={connectionStringsQuery.isError}
                errorTitle="Could not load connection strings"
                onRetry={refetch}
                loadingLabel="Loading connection strings..."
                skeleton={
                    <ConnectionStringsTableFrame>
                        <TableSkeletonRows columnCount={CONNECTION_STRINGS_COLUMN_COUNT} rows={4} hasActionColumn />
                    </ConnectionStringsTableFrame>
                }
            >
                <ConnectionStringsTableFrame>
                    {items.length === 0 ? (
                        <TableRow className="hover:bg-transparent">
                            <TableCell
                                colSpan={CONNECTION_STRINGS_COLUMN_COUNT}
                                className="h-20 text-center text-muted-foreground"
                            >
                                No connection strings yet.
                            </TableCell>
                        </TableRow>
                    ) : (
                        items.map((item) => {
                            const name = item.name ?? "";
                            const modelType = item.modelType ?? "Chat";
                            return (
                                <TableRow key={name}>
                                    <TableCell className="font-medium">{name}</TableCell>
                                    <TableCell>{getProviderLabel(item)}</TableCell>
                                    <TableCell>
                                        <Badge variant="secondary">{MODEL_TYPE_LABELS[modelType]}</Badge>
                                    </TableCell>
                                    <TableCell className="text-right">
                                        <div className="flex justify-end gap-1">
                                            <EditAiConnectionString
                                                name={name}
                                                modelType={modelType}
                                                onSaved={refetch}
                                                trigger={
                                                    <Button variant="ghost" size="icon-sm" aria-label={`Edit ${name}`}>
                                                        <Pencil className="size-3.5" aria-hidden="true" />
                                                    </Button>
                                                }
                                            />
                                            <DeleteAiConnectionStringDialog
                                                name={name}
                                                trigger={
                                                    <Button
                                                        variant="ghost"
                                                        size="icon-sm"
                                                        aria-label={`Delete ${name}`}
                                                    >
                                                        <Trash2 className="size-3.5" aria-hidden="true" />
                                                    </Button>
                                                }
                                            />
                                        </div>
                                    </TableCell>
                                </TableRow>
                            );
                        })
                    )}
                </ConnectionStringsTableFrame>
            </ApiState>
        </div>
    );
}

function ConnectionStringsTableFrame({ children }: { children: ReactNode }) {
    return (
        <div className="overflow-hidden rounded-lg border">
            <Table>
                <TableHeader>
                    <TableRow className="hover:bg-transparent">
                        <TableHead className="text-xs font-medium text-muted-foreground">Name</TableHead>
                        <TableHead className="text-xs font-medium text-muted-foreground">Provider</TableHead>
                        <TableHead className="text-xs font-medium text-muted-foreground">Model type</TableHead>
                        <TableHead className="w-0 text-right text-xs font-medium text-muted-foreground">
                            <span className="sr-only">Actions</span>
                        </TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>{children}</TableBody>
            </Table>
        </div>
    );
}
