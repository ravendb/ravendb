import { StorageReportItem } from "components/pages/resources/manageServer/storageReport/partials/common";
import { HierarchyNode } from "d3-hierarchy";
import { withPreventDefault } from "components/utils/common";

interface StorageReportPathProps {
    node: StorageReportItem;
    rootHierarchy: HierarchyNode<StorageReportItem>;
    onNodeSelected: (node: StorageReportItem) => void;
}

export function StorageReportPath(props: StorageReportPathProps) {
    const { node, rootHierarchy, onNodeSelected } = props;

    const currentPath = rootHierarchy
        .find((x) => x.data === node)
        .ancestors()
        .map((x) => x.data)
        .reverse();

    return (
        <div className="current-path">
            {currentPath.map((path, i) => (
                <span key={i}>
                    <a href="#" className={path.type} onClick={withPreventDefault(() => onNodeSelected(path))}>
                        <small>{path.type}</small>
                        <span>{path.name}</span>
                    </a>
                    {i < currentPath.length - 1 && <i className="icon-arrow-filled-right"></i>}
                </span>
            ))}
        </div>
    );
}
