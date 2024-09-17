import { IndexSharedInfo } from "components/models/indexes";
import ExportIndexesListItem from "components/pages/database/indexes/list/migration/export/ExportIndexesListItem";
import React from "react";
import { ListGroup } from "reactstrap";

interface ExportIndexesListProps {
    availableIndexes: IndexSharedInfo[];
    unavailableIndexes: IndexSharedInfo[];
    disabledReason?: string;
}

export default function ExportIndexesList({
    availableIndexes,
    unavailableIndexes,
    disabledReason,
}: ExportIndexesListProps) {
    if (!availableIndexes?.length && !unavailableIndexes?.length) {
        return null;
    }

    return (
        <div className="vstack gap-3 overflow-auto" style={{ maxHeight: "200px" }}>
            <ListGroup>
                {availableIndexes.map((index) => (
                    <ExportIndexesListItem key={index.name} index={index} />
                ))}
                {unavailableIndexes.map((index) => (
                    <ExportIndexesListItem key={index.name} index={index} disabledReason={disabledReason} />
                ))}
            </ListGroup>
        </div>
    );
}
