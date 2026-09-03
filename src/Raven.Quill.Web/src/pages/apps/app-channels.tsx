import { useParams } from "react-router";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import { ChannelGroups } from "@/pages/apps/channel-groups";
import { Heading, Text } from "@/components/typography";

export function AppChannels() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                    <Heading as="h1" variant="page">
                        Channels
                    </Heading>
                    <Text variant="muted" className="max-w-prose">
                        Channels are the surfaces end users reach your agents through.
                    </Text>
                </div>
                <AddChannelMenu slug={slug} label="New channel" variant="default" />
            </div>
            <ChannelGroups slug={slug} />
        </div>
    );
}
