import React, { useCallback, useLayoutEffect, useState } from "react";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import ClusterDebugAboutView from "./partials/ClusterDebugAboutView";
import ClusterDebugSummary from "./partials/ClusterDebugSummary";
import ClusterDebugEntries from "./partials/ClusterDebugEntries";
import { useServices } from "hooks/useServices";
import { useClusterWideAsync } from "hooks/useClusterWideAsync";
import ClusterDebugGlobalInfo from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterDebugGlobalInfo";
import { mapRaftDebugView } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import SizeGetter from "components/common/SizeGetter";

export default function ClusterDebug() {
    const { manageServerService } = useServices();
    const [refreshing, setRefreshing] = useState(false);

    const getAndMapLog = useCallback(
        async (nodeTag: string) => {
            const result = await manageServerService.getClusterLog(nodeTag, 0, 1);
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

    useLayoutEffect(() => {
        // FIXME: hack to allow bs5 and bs3 with child router in durandal
        const bs3Container = document.querySelector(".content-container.bs3");
        if (bs3Container) {
            bs3Container.classList.remove("bs3");
            bs3Container.querySelector(".nav.nav-tabs")?.parentElement.classList.add("bs3");
        }
    }, []);

    return (
        <div className="flex-window padding-xs">
            <div className="bs5">
                <div className="flex-shrink-0 hstack gap-2 align-items-start">
                    <AboutViewHeading title="Cluster Debug" icon="cluster-debug" />
                    <FlexGrow />
                    <ClusterDebugAboutView />
                </div>
                <div className="d-flex align-items-start gap-3 flex-wrap">
                    <ButtonWithSpinner onClick={handleRefresh} variant="primary" isSpinning={refreshing} icon="refresh">
                        Refresh
                    </ButtonWithSpinner>
                    <FlexGrow />
                    <ClusterDebugGlobalInfo nodes={result} />
                </div>
                <h3 className="mt-3">Summary</h3>
                <ClusterDebugSummary nodes={result} />
                <h3 className="hstack align-items-center mt-4">Log Entries</h3>
                <SizeGetter render={(size) => <ClusterDebugEntries availableWidth={size.width} nodes={result} />} />
            </div>
        </div>
    );
}
