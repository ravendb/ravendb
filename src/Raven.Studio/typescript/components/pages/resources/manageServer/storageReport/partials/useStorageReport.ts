import { useAsync } from "react-async-hook";
import { useCallback, useState } from "react";
import {
    createHierarchyWithValues,
    hasChildren,
    mapReport,
    StorageReportItem,
} from "components/pages/resources/manageServer/storageReport/partials/common";
import { HierarchyNode } from "d3-hierarchy";
import { useServices } from "hooks/useServices";

export function useStorageReport() {
    const { manageServerService } = useServices();

    const [rootData, setRootData] = useState<StorageReportItem>();
    const [node, setNode] = useState<StorageReportItem>();
    const [rootHierarchy, setRootHierarchy] = useState<HierarchyNode<StorageReportItem>>();

    const { status, execute } = useAsync(manageServerService.getSystemStorageReport, [], {
        onSuccess: (data) => {
            const mappedReport = mapReport(data);
            setRootData(mappedReport);
            setRootHierarchy(createHierarchyWithValues(mappedReport));
            setNode(mappedReport);
        },
    });

    const onClick = useCallback(
        (newNode: StorageReportItem) => {
            if (node === newNode) {
                return;
            }

            if (!hasChildren(newNode)) {
                // it is a leaf node - prevent click
                return;
            }
            setNode(newNode);
        },
        [node]
    );

    return {
        rootData,
        fetchStatus: status,
        reload: execute,
        node,
        onClick,
        rootHierarchy,
    };
}
