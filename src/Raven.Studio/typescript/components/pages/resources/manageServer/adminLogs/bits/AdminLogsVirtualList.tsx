import { useVirtualizer } from "@tanstack/react-virtual";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import AdminLogsBufferAlert from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsBufferAlert";
import {
    adminLogsSelectors,
    adminLogsActions,
    AdminLogsMessage,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import { useRef, useLayoutEffect } from "react";
import Table from "react-bootstrap/Table";

export default function AdminLogsVirtualList(props: { availableHeightInPx: number }) {
    const dispatch = useAppDispatch();

    const filteredLogs = useAppSelector(adminLogsSelectors.filteredLogs);
    const isMonitorTail = useAppSelector(adminLogsSelectors.isMonitorTail);

    const listRef = useRef<HTMLDivElement>(null);

    const filteredLogsLength = filteredLogs.length;

    const virtualizer = useVirtualizer({
        count: filteredLogsLength,
        estimateSize: () => 22,
        getScrollElement: () => listRef.current,
        overscan: 5,
        measureElement: (element) => {
            return element.getBoundingClientRect().height;
        },
        getItemKey: (index) => filteredLogs[index]._meta.id,
    });

    // Scroll to bottom if isMonitorTail is true
    useLayoutEffect(() => {
        if (isMonitorTail) {
            virtualizer.scrollToIndex(filteredLogsLength - 1);
        }
    }, [isMonitorTail, virtualizer, filteredLogsLength]);

    return (
        <div ref={listRef} style={{ overflow: "auto", height: props.availableHeightInPx }}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                <AdminLogsBufferAlert />
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const log = filteredLogs[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            className="hover-filter"
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                padding: "2px 0px",
                                transition: "unset",
                            }}
                        >
                            <div
                                style={{
                                    borderLeft: `4px solid ${getTextColor(log.Level)}`,
                                    backgroundColor: `var(--panel-bg-1)`,
                                }}
                            >
                                <div
                                    key={log.Date}
                                    className="d-flex align-items-center cursor-pointer text-truncate"
                                    onClick={() => {
                                        if (!log._meta.isExpanded && isMonitorTail) {
                                            dispatch(adminLogsActions.isMonitorTailToggled());
                                        }
                                        dispatch(adminLogsActions.isLogExpandedToggled(log));
                                    }}
                                    style={{ padding: `4.3px` }}
                                >
                                    <div style={{ margin: "0 4.3px 0 0" }}>
                                        <Icon
                                            icon={log._meta.isExpanded ? "chevron-down" : "chevron-right"}
                                            className="fs-6"
                                            margin="m-0"
                                        />
                                    </div>
                                    <span className="text-truncate">
                                        {log.Date} | {log.Level} | {log.Resource} | {log.Component} | {log.Message}
                                    </span>
                                </div>
                                {log._meta.isExpanded && (
                                    <div className="vstack gap-2 p-2">
                                        <Code
                                            code={log.Message}
                                            elementToCopy={log.Message}
                                            language="plaintext"
                                            codeClassName="wrapped pe-4"
                                        />
                                        <div className="p-2">
                                            <Table size="sm" className="m-0">
                                                <tbody>
                                                    {Object.keys(log)
                                                        .filter(
                                                            (key: keyof AdminLogsMessage) =>
                                                                key !== "_meta" && key !== "Message"
                                                        )
                                                        .map((key: keyof AdminLogsMessage) => (
                                                            <tr key={key}>
                                                                <td>{getFormattedFieldName(key)}</td>
                                                                <td>{String(log[key] ?? "-")}</td>
                                                            </tr>
                                                        ))}
                                                </tbody>
                                            </Table>
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function getTextColor(level: AdminLogsMessage["Level"]): string {
    switch (level) {
        case "DEBUG":
            return "var(--bs-success)";
        case "INFO":
            return "var(--bs-info)";
        case "WARN":
            return "var(--bs-warning)";
        case "ERROR":
            return "var(--bs-orange)";
        case "FATAL":
            return "var(--bs-danger)";
        case "OFF":
        case "TRACE":
            return "var(--panel-bg-3)";
        default:
            return assertUnreachable(level);
    }
}

function getFormattedFieldName(fieldName: keyof AdminLogsMessage): string {
    switch (fieldName) {
        case "ThreadID":
            return "Thread ID";
        default:
            return fieldName;
    }
}
