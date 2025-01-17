import { StorageReportGraph } from "components/pages/resources/manageServer/storageReport/partials/StorageReportGraph";
import { StorageReportTable } from "components/pages/resources/manageServer/storageReport/partials/StorageReportTable";
import { StorageReportPath } from "components/pages/resources/manageServer/storageReport/partials/StorageReportPath";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { useStorageReport } from "components/pages/resources/manageServer/storageReport/partials/useStorageReport";
import { LoadError } from "components/common/LoadError";

export default function StorageReport() {
    const { rootData, node, fetchStatus, reload, onClick, rootHierarchy } = useStorageReport();

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to Server Storage Report" refresh={reload} />;
    }

    return (
        <div className="bs3">
            <div id="storage-report" className="content-margin">
                <StorageReportGraph node={node} rootHierarchy={rootHierarchy} />
                <StorageReportPath
                    node={node}
                    rootHierarchy={rootHierarchy}
                    onNodeSelected={(node) => onClick(node, false)}
                />
                <hr />
                <StorageReportTable onNodeSelected={(node) => onClick(node, true)} node={node} root={rootData} />
            </div>
        </div>
    );
}
