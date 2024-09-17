import CheckboxSelectAll from "components/common/CheckboxSelectAll";
import { SelectionState } from "components/models/common";
import ImportIndexListItem from "components/pages/database/indexes/list/migration/import/ImportIndexListItem";
import React from "react";
import { ListGroup } from "reactstrap";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;

interface ImportIndexesListProps {
    availableIndexes: IndexDefinition[];
    unavailableIndexes: IndexDefinition[];
    disabledReason?: string;
    selectionState: SelectionState;
    selectedIndexNames: string[];
    toggleAllIndexNames: () => void;
    toggleIndexName: (indexName: string) => void;
}

export default function ImportIndexesList({
    availableIndexes,
    unavailableIndexes,
    disabledReason,
    selectionState,
    selectedIndexNames,
    toggleAllIndexNames,
    toggleIndexName,
}: ImportIndexesListProps) {
    if (!availableIndexes?.length && !unavailableIndexes?.length) {
        return null;
    }

    return (
        <div>
            <CheckboxSelectAll
                selectionState={selectionState}
                toggleAll={toggleAllIndexNames}
                allItemsCount={availableIndexes.length}
                selectedItemsCount={selectedIndexNames.length}
            />
            <div className="vstack gap-3 overflow-auto" style={{ maxHeight: "340px" }}>
                <ListGroup>
                    {availableIndexes.map((definition) => (
                        <ImportIndexListItem
                            key={definition.Name}
                            indexDefinition={definition}
                            toggleIndexName={toggleIndexName}
                            selectedIndexNames={selectedIndexNames}
                        />
                    ))}
                    {unavailableIndexes.map((definition) => (
                        <ImportIndexListItem
                            key={definition.Name}
                            indexDefinition={definition}
                            toggleIndexName={toggleIndexName}
                            selectedIndexNames={selectedIndexNames}
                            disabledReason={disabledReason}
                        />
                    ))}
                </ListGroup>
            </div>
        </div>
    );
}
