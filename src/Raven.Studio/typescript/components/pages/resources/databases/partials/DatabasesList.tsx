import { useVirtualizer } from "@tanstack/react-virtual";
import { useRef } from "react";
import { DatabasePanel } from "./DatabasePanel";

interface DatabasesListProps {
    filteredDatabaseNames: string[];
    selectedDatabaseNames: string[];
    toggleSelection: (dbName: string) => void;
}

export default function DatabasesList({
    filteredDatabaseNames,
    selectedDatabaseNames,
    toggleSelection,
}: DatabasesListProps) {
    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: filteredDatabaseNames.length,
        estimateSize: () => 214,
        getScrollElement: () => listRef.current,
        overscan: 2,
        measureElement: (element: HTMLElement) => {
            return element.getBoundingClientRect().height;
        },
        getItemKey: (index: number) => filteredDatabaseNames[index],
    });

    return (
        <div ref={listRef} className="h-100 overflow-auto px-4">
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const dbName = filteredDatabaseNames[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                            }}
                            className="pb-2 pt-1 virtual-item"
                        >
                            <DatabasePanel
                                key={dbName}
                                databaseName={dbName}
                                selected={selectedDatabaseNames.includes(dbName)}
                                toggleSelection={() => toggleSelection(dbName)}
                            />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
