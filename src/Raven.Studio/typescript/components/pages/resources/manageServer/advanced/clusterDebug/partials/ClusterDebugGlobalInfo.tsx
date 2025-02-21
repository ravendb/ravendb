import { Icon } from "components/common/Icon";
import React from "react";
import { nodeAwareLoadableData } from "hooks/useClusterWideAsync";
import { ClusterDebugNodeInfo } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";

interface ClusterDebugGlobalInfoProps {
    nodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[];
}
export default function ClusterDebugGlobalInfo(props: ClusterDebugGlobalInfoProps) {
    const { nodes } = props;
    const hasAnyData = nodes.some((x) => x.status === "success");
    const successNodes = nodes.filter((x) => x.status === "success");
    const term = hasAnyData ? Math.max(...successNodes.map((x) => x.data.term)) : "?";
    const clusterVersion = hasAnyData ? Math.max(...successNodes.map((x) => x.data.clusterVersion)) : "?";

    return (
        <div className="d-flex gap-3 flex-wrap">
            <div>
                <div className="card p-2 border-radius-xs vstack">
                    <small className="small-label">
                        <Icon icon="document" />
                        Term
                    </small>
                    <h5 className="mt-1 mb-0">
                        <strong>{term}</strong>
                    </h5>
                </div>
            </div>
            <div>
                <div className="card p-2 border-radius-xs vstack">
                    <small className="small-label">
                        <Icon icon="cluster" />
                        Cluster version
                    </small>
                    <h5 className="mt-1 mb-0">
                        <strong>{clusterVersion}</strong>
                    </h5>
                </div>
            </div>
        </div>
    );
}
