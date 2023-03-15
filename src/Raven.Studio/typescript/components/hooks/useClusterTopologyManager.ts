import { useEffect, useState } from "react";
import clusterTopologyManager from "common/shell/clusterTopologyManager";

//TODO: move to redux
export function useClusterTopologyManager() {
    const [nodeTags, setNodeTags] = useState<string[]>(
        clusterTopologyManager.default
            .topology()
            ?.nodes()
            ?.map((x) => x.tag()) ?? []
    );
    const [localNodeTag, setLocalNodeTag] = useState<string>(clusterTopologyManager.default.localNodeTag());

    useEffect(() => {
        const sub = clusterTopologyManager.default.localNodeTag.subscribe((tag) => {
            setLocalNodeTag(tag);
        });

        return () => sub.dispose();
    }, []);

    useEffect(() => {
        const sub = clusterTopologyManager.default.topology.subscribe((topology) => {
            setNodeTags(topology.nodes().map((x) => x.tag()));
        }, []);

        return () => sub.dispose();
    });

    return {
        nodeTags,
        localNodeTag,
    };
}
