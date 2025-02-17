import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import ClusterDebugAboutView from "components/pages/resources/cluster/clusterDebug/ClusterDebugAboutView";
import ClusterDebugSummary from "components/pages/resources/cluster/clusterDebug/ClusterDebugSummary";
import ClusterDebugEntries from "components/pages/resources/cluster/clusterDebug/ClusterDebugEntries";

interface ClusterDebugProps {}

export default function ClusterDebug(props: ClusterDebugProps) {
    return (
        <>
            <div className="flex-shrink-0 hstack gap-2 align-items-start">
                <AboutViewHeading title="Cluster Debug" icon="cluster-debug" />
                <FlexGrow />
                <ClusterDebugAboutView />
            </div>
            <div className="d-flex align-items-start gap-3 flex-wrap">
                <ButtonWithSpinner onClick={null} color="primary" isSpinning={null} icon="refresh">
                    Refresh
                </ButtonWithSpinner>
                <FlexGrow />
                <div className="d-flex gap-3 flex-wrap">
                    <div>
                        <div className="card p-2 border-radius-xs vstack">
                            <small className="small-label">
                                <Icon icon="document" />
                                Term
                            </small>
                            <h5 className="mt-1 mb-0">
                                <strong>35</strong>
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
                                <strong>60_000</strong>
                            </h5>
                        </div>
                    </div>
                </div>
            </div>
            <h3 className="mt-3">Summary</h3>
            <ClusterDebugSummary />
            <h3 className="hstack align-items-center mt-4">
                Entries
                <a href="#" className="no-decor fs-4" title="See json">
                    <Icon icon="json" margin="ms-1" />
                </a>
            </h3>
            <ClusterDebugEntries />
        </>
    );
}
