import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { RawDataPreview } from "@/components/data/raw-data-preview";
import { ChannelsSection } from "@/pages/apps/channels-section";

export function AppChannels() {
    const { slug = "" } = useParams();
    const channelStatsQuery = useQuery(api.queries.stats.channels(slug));

    return (
        <div className="space-y-5">
            <ChannelsSection slug={slug} />
            <RawDataPreview title="stats.channels" query={channelStatsQuery} />
        </div>
    );
}
