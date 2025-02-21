import { hasChildren, StorageReportItem } from "components/pages/resources/manageServer/storageReport/partials/common";
import genUtils from "common/generalUtils";
import React from "react";
import { withPreventDefault } from "components/utils/common";
import { UncontrolledTooltip } from "reactstrap";
import useUniqueId from "hooks/useUniqueId";

interface StorageReportTableProps {
    node: StorageReportItem;
    root: StorageReportItem;
    onNodeSelected: (node: StorageReportItem) => void;
}

export function StorageReportTable(props: StorageReportTableProps) {
    const { node, root, onNodeSelected } = props;
    const tooltipId = useUniqueId("tooltip");

    if (!node || !node.internalChildren) {
        return null;
    }

    const showPagesColumn = node.internalChildren.some((x) => x.type === "tree");
    const showEntriesColumn = node.internalChildren.some((x) => x.type === "table" || x.type === "tree");
    const showTempFiles = node === root;

    return (
        <table className="table table-condensed table-striped on-base-background">
            <thead>
                <tr>
                    <th>Type</th>
                    <th className="column-min-width">Name</th>
                    {showPagesColumn && <th key="pages-column"># Pages</th>}
                    {showEntriesColumn && <th key="entries">Entries</th>}
                    <th>
                        {showTempFiles && <>Total</>}
                        Size (&sum; <FormattedSize node={node} header />)
                    </th>
                    <th>% Total</th>
                </tr>
            </thead>
            <tbody>
                {node.internalChildren.map((item, index) => (
                    <tr key={index}>
                        <td>{_.upperFirst(item.type)}</td>
                        <td className="position-relative">
                            <div className="table-items">
                                {hasChildren(item) ? (
                                    <a
                                        href="#"
                                        className="table-item-name text-elipsis"
                                        title={item.name}
                                        onClick={withPreventDefault(() => onNodeSelected(item))}
                                    >
                                        {item.name}
                                    </a>
                                ) : (
                                    <span title={item.name} className="table-item-name text-elipsis">
                                        {item.name}
                                    </span>
                                )}
                                {item.recyclableJournal && (
                                    <span>
                                        <small id={tooltipId + "-" + index}>
                                            <i className="icon-info text-info"></i>
                                        </small>
                                        <UncontrolledTooltip target={tooltipId + "-" + index}>
                                            Stored in Journals directory
                                        </UncontrolledTooltip>
                                    </span>
                                )}
                            </div>
                        </td>
                        {showPagesColumn && (
                            <td key="page-count">{item.pageCount ? item.pageCount.toLocaleString() : 0}</td>
                        )}
                        {showEntriesColumn && (
                            <td key="entries">{item.numberOfEntries ? item.numberOfEntries.toLocaleString() : 0}</td>
                        )}
                        <td>
                            <FormattedSize node={item} header={false} />
                        </td>
                        <td>{formatPercentage(item, node.size)}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

function FormattedSize(props: { node: StorageReportItem; header: boolean }) {
    const { header, node } = props;
    if (node.customSizeProvider) {
        const value = node.customSizeProvider(header);
        return <span title={value.title}>{value.text}</span>;
    }
    return <span>{genUtils.formatBytesToSize(node.size)}</span>;
}

function formatPercentage(node: StorageReportItem, parentSize: number) {
    return ((node.size * 100) / parentSize).toFixed(2) + "%";
}
