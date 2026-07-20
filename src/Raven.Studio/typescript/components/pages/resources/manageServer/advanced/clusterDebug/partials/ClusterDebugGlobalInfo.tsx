import { Icon } from "components/common/Icon";
import React from "react";
import classNames from "classnames";
import { ClusterDebugNodeInfo } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import { nodeAwareLoadableData } from "components/models/common";

interface ClusterDebugGlobalInfoProps {
    nodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[];
    cardClassName?: string;
    fillWidth?: boolean;
}
export default function ClusterDebugGlobalInfo(props: ClusterDebugGlobalInfoProps) {
    const { nodes, cardClassName, fillWidth } = props;
    const hasAnyData = nodes.some((x) => x.status === "success");
    const successNodes = nodes.filter((x) => x.status === "success");
    const term = hasAnyData ? Math.max(...successNodes.map((x) => x.data.term)) : "?";
    const clusterVersion = hasAnyData ? Math.max(...successNodes.map((x) => x.data.clusterVersion)) : "?";

    const wrapperClassName = classNames({ "flex-grow-1": fillWidth });

    return (
        <div className="d-flex gap-2 flex-wrap">
            <div className={wrapperClassName}>
                <div className={classNames("card p-2 border-radius-xs vstack", cardClassName)}>
                    <small className="small-label">
                        <Icon icon="document" />
                        Term
                    </small>
                    <h5 className="mt-1 mb-0">
                        <strong>{term}</strong>
                    </h5>
                </div>
            </div>
            <div className={wrapperClassName}>
                <div className={classNames("card p-2 border-radius-xs vstack", cardClassName)}>
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
