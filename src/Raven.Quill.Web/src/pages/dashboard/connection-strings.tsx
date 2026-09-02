import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { api } from "@/api/api";
import type { AiConnectionStringUsage } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { TableSkeletonRows } from "@/components/table/table-skeleton";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import { EditAiConnectionString } from "@/components/ai-connection-string/edit-ai-connection-string";
import { DeleteAiConnectionStringDialog } from "@/components/ai-connection-string/delete-ai-connection-string-dialog";
import { AiConnectionStringUsageList } from "@/components/ai-connection-string/ai-connection-string-usage";
import { getProviderLabel, MODEL_TYPE_LABELS } from "@/components/ai-connection-string/ai-connection-string-utils";
import { Heading, Text } from "@/components/typography";

const CONNECTION_STRINGS_COLUMN_COUNT = 4;

export function DashboardConnectionStrings() {
    const connectionStringsQuery = useQuery(api.queries.aiConnectionStrings.list());
    const items = connectionStringsQuery.data ?? [];

    const refetch = () => void connectionStringsQuery.refetch();

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                    <Heading as="h1" variant="page">
                        AI connection strings
                    </Heading>
                    <Text variant="muted">
                        Provider credentials agents use to reach a model, shared by every app on this server.
                    </Text>
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
                        items.map(({ connectionString, usedBy }) => {
                            const name = connectionString.name ?? "";
                            const modelType = connectionString.modelType ?? "Chat";
                            return (
                                <TableRow key={name}>
                                    <TableCell className="font-medium">{name}</TableCell>
                                    <TableCell>{getProviderLabel(connectionString)}</TableCell>
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
                                            <DeleteConnectionStringAction name={name} usedBy={usedBy} />
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

type DeleteConnectionStringActionProps = {
    name: string;
    usedBy: AiConnectionStringUsage[];
};

function DeleteConnectionStringAction({ name, usedBy }: DeleteConnectionStringActionProps) {
    if (usedBy.length === 0) {
        return (
            <DeleteAiConnectionStringDialog
                name={name}
                trigger={
                    <Button variant="ghost" size="icon-sm" aria-label={`Delete ${name}`}>
                        <Trash2 className="size-3.5" aria-hidden="true" />
                    </Button>
                }
            />
        );
    }

    return (
        <Tooltip>
            {/* The tooltip is the only place the reason appears, so the button stays focusable
                (aria-disabled, not disabled) and keeps carrying its own name and description. */}
            <TooltipTrigger asChild>
                <Button
                    variant="ghost"
                    size="icon-sm"
                    aria-label={`Delete ${name}`}
                    aria-disabled
                    className="aria-disabled:opacity-50 aria-disabled:hover:bg-transparent"
                >
                    <Trash2 className="size-3.5" aria-hidden="true" />
                </Button>
            </TooltipTrigger>
            <TooltipContent>
                <div className="grid gap-1">
                    <span>This connection string is in use and can’t be deleted. Used by:</span>
                    <AiConnectionStringUsageList usedBy={usedBy} />
                </div>
            </TooltipContent>
        </Tooltip>
    );
}

function ConnectionStringsTableFrame({ children }: { children: ReactNode }) {
    return (
        <TooltipProvider>
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
        </TooltipProvider>
    );
}
