import React, { useCallback, useState } from "react";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import ClusterDebugAboutView from "./partials/ClusterDebugAboutView";
import ClusterDebugSummary from "./partials/ClusterDebugSummary";
import ClusterDebugEntries from "./partials/ClusterDebugEntries";
import { useServices } from "hooks/useServices";
import { useClusterWideAsync } from "hooks/useClusterWideAsync";
import ClusterDebugGlobalInfo from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterDebugGlobalInfo";
import { mapRaftDebugView } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";

export default function ClusterDebug() {
    const { manageServerService } = useServices();
    const [refreshing, setRefreshing] = useState(false);

    const getAndMapLog = useCallback(
        async (nodeTag: string) => {
            const result = await manageServerService.getClusterLog(nodeTag);
            return mapRaftDebugView(result);
        },
        [manageServerService]
    );

    const { result, refresh } = useClusterWideAsync(getAndMapLog);

    const handleRefresh = async () => {
        setRefreshing(true);
        try {
            await refresh();
        } finally {
            setRefreshing(false);
        }
    };

    return (
        <div className="flex-window padding-xs">
            <div className="bs5">
                <div className="flex-shrink-0 hstack gap-2 align-items-start">
                    <AboutViewHeading title="Cluster Debug" icon="cluster-debug" />
                    <FlexGrow />
                    <ClusterDebugAboutView />
                </div>
                <div className="d-flex align-items-start gap-3 flex-wrap">
                    <ButtonWithSpinner onClick={handleRefresh} color="primary" isSpinning={refreshing} icon="refresh">
                        Refresh
                    </ButtonWithSpinner>
                    <FlexGrow />
                    <ClusterDebugGlobalInfo nodes={result} />
                </div>
                <h3 className="mt-3">Summary</h3>
                <ClusterDebugSummary nodes={result} />
                <h3 className="hstack align-items-center mt-4">
                    Entries
                    <a href="#" className="no-decor fs-4" title="See json">
                        <Icon icon="json" margin="ms-1" />
                    </a>
                </h3>
                <ClusterDebugEntries />
            </div>
        </div>
    );
}
