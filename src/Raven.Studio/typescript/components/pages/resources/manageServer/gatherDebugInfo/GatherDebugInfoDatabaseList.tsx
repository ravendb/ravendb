import React, { useRef } from "react";
import Form from "react-bootstrap/Form";
import { useVirtualizer } from "@tanstack/react-virtual";

interface GatherDebugInfoDatabaseListProps {
    databaseNames: string[];
    selectedDatabases: string[];
    onToggle: (dbName: string) => void;
}

// the cluster can hold thousands of databases, so the list is virtualized to keep only the visible rows mounted
export default function GatherDebugInfoDatabaseList({
    databaseNames,
    selectedDatabases,
    onToggle,
}: GatherDebugInfoDatabaseListProps) {
    const scrollRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: databaseNames.length,
        estimateSize: () => 41,
        getScrollElement: () => scrollRef.current,
        overscan: 10,
    });

    if (databaseNames.length === 0) {
        return (
            <div className="gather-debug-info-db-scroll">
                <div className="py-3 text-center text-muted">No databases found</div>
            </div>
        );
    }

    return (
        <div className="gather-debug-info-db-scroll" ref={scrollRef}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const db = databaseNames[virtualRow.index];

                    return (
                        <div
                            key={db}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            className="gather-debug-info-db-item"
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                            }}
                        >
                            <span className="text-truncate me-3">{db}</span>
                            <Form.Check
                                type="switch"
                                checked={selectedDatabases.includes(db)}
                                onChange={() => onToggle(db)}
                                id={`db-toggle-${db.replace(/[^a-zA-Z0-9]/g, "-")}`}
                                label=""
                                className="m-0"
                            />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
