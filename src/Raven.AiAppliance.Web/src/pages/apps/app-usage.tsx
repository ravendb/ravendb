import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { PagePanel } from "@/components/data/page-panel";
import { RawDataPreview } from "@/components/data/raw-data-preview";

export function AppUsage() {
    const { slug = "" } = useParams();
    const appUsageQuery = useQuery(api.queries.stats.appUsage(slug));
    const monthlyWritesQuery = useQuery(api.queries.settings.usage());

    return (
        <PagePanel>
            <div className="space-y-6">
                <RawDataPreview title="stats.appUsage" query={appUsageQuery} />
                <RawDataPreview title="settings.usage" query={monthlyWritesQuery} />
            </div>
        </PagePanel>
    );
}
