import { useState } from "react";
import { useNavigate, useParams } from "react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { CalendarClock, Database, DownloadIcon, Pencil, Server, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { api } from "@/api/api";
import { DetailHeader, DetailHeaderMenu, DetailHeaderMetaItem } from "@/components/data/detail-header";
import { StatusIndicator } from "@/components/data/status-indicator";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { DropdownMenuItem } from "@/components/shadcn/ui/dropdown-menu";
import { resolveStatusStyle } from "@/lib/app-status";
import { formatDate } from "@/lib/format";
import { CdcPerformanceSection } from "@/pages/apps/cdc-performance-section";
import { CollectionsSection } from "@/pages/apps/collections-section";
import { appRoutes } from "@/lib/app-routes";
import { DeleteAppDialog } from "@/pages/apps/delete-app-dialog";
import { buildConfigExportFromCdc, downloadConfig } from "@/pages/setup/add-app-wizard/config-io";
import { PROVIDER_OPTIONS } from "@/pages/setup/add-app-wizard/steps/connect/connect-source-options";

export function AppDataSource() {
    const { slug = "" } = useParams();
    const appQuery = useQuery(api.queries.apps.detail(slug));
    const dashboardAppQuery = useQuery(api.queries.stats.dashboardApp(slug));

    const sourceType = dashboardAppQuery.data?.source.type ?? "";
    // Only external database sources carry a portable connection + mapping, matching the wizard's gate.
    const isExternalSource = PROVIDER_OPTIONS.some((option) => option.label === sourceType);

    return (
        <div className="flex h-full min-h-0 flex-col">
            <DetailHeader
                title="Data source"
                status={
                    <DataSourceStatus status={dashboardAppQuery.data?.status} isLoading={dashboardAppQuery.isPending} />
                }
                meta={
                    appQuery.data && (
                        <>
                            {sourceType && (
                                <DetailHeaderMetaItem icon={Server} tooltip="Source engine">
                                    {sourceType}
                                </DetailHeaderMetaItem>
                            )}
                            <DetailHeaderMetaItem icon={Database} mono tooltip="Source database">
                                {appQuery.data.database}
                            </DetailHeaderMetaItem>
                            <DetailHeaderMetaItem icon={CalendarClock} tooltip="Connected">
                                {formatDate(appQuery.data.createdAt)}
                            </DetailHeaderMetaItem>
                        </>
                    )
                }
                actions={
                    appQuery.data && (
                        <DataSourceActions
                            slug={slug}
                            appName={appQuery.data.name}
                            sourceType={sourceType}
                            canExport={isExternalSource}
                        />
                    )
                }
            />

            {/* py-5 lives inside the scroller so it breathes at rest but scrolls away (content stays
                flush with the header while scrolling). -mx-2/px-2 is visually self-cancelling but keeps
                card borders and shadows off the clip edge (overflow-y-auto clips the x-axis too). */}
            <div className="-mx-2 min-h-0 flex-1 space-y-8 overflow-y-auto px-2 py-5">
                <CdcPerformanceSection slug={slug} />
                <CollectionsSection slug={slug} />
            </div>
        </div>
    );
}

function DataSourceStatus({ status, isLoading }: { status: string | undefined; isLoading: boolean }) {
    if (isLoading) {
        return <StatusIndicator tone="loading" label="Loading" />;
    }
    if (status === undefined) {
        return null;
    }
    const style = resolveStatusStyle(status);
    return <StatusIndicator tone={style.tone} label={style.label} />;
}

function DataSourceActions({
    slug,
    appName,
    sourceType,
    canExport,
}: {
    slug: string;
    appName: string;
    sourceType: string;
    canExport: boolean;
}) {
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [isExportOpen, setIsExportOpen] = useState(false);
    const [isDeleteOpen, setIsDeleteOpen] = useState(false);

    const onExport = async () => {
        try {
            const cdc = await queryClient.fetchQuery(api.queries.apps.cdcGet(slug));
            downloadConfig(buildConfigExportFromCdc(sourceType, cdc));
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Could not export the configuration.");
        }
    };

    return (
        <>
            <DetailHeaderMenu>
                <DropdownMenuItem onSelect={() => navigate(appRoutes.editApp(slug))}>
                    <Pencil aria-hidden="true" />
                    Edit
                </DropdownMenuItem>
                {canExport && (
                    <DropdownMenuItem onSelect={() => setIsExportOpen(true)}>
                        <DownloadIcon aria-hidden="true" />
                        Export configuration
                    </DropdownMenuItem>
                )}
                <DropdownMenuItem variant="destructive" onSelect={() => setIsDeleteOpen(true)}>
                    <Trash2 aria-hidden="true" />
                    Delete
                </DropdownMenuItem>
            </DetailHeaderMenu>

            <ConfirmDialog
                open={isExportOpen}
                onOpenChange={setIsExportOpen}
                variant="warning"
                title="Export configuration?"
                description="The exported file contains the connection string in plain text, including any username and password it holds. Keep it somewhere safe and avoid sharing it."
                confirmLabel="Export"
                onConfirm={onExport}
            />

            <DeleteAppDialog slug={slug} appName={appName} open={isDeleteOpen} onOpenChange={setIsDeleteOpen} />
        </>
    );
}
