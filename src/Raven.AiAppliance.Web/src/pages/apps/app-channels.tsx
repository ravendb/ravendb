import { useParams } from "react-router";
import { ChannelsSection } from "@/pages/apps/channels-section";

export function AppChannels() {
    const { slug = "" } = useParams();

    return <ChannelsSection slug={slug} />;
}
