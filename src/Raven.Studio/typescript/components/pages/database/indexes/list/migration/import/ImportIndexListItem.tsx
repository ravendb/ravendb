import { Checkbox } from "components/common/Checkbox";
import IndexToMigrateTitle from "components/pages/database/indexes/list/migration/common/IndexToMigrateTitle";
import React from "react";
import { ListGroupItem, Label } from "reactstrap";

interface ImportIndexListItemProps {
    indexDefinition: Raven.Client.Documents.Indexes.IndexDefinition;
    toggleIndexName: (indexName: string) => void;
    selectedIndexNames: string[];
    disabledReason?: string;
}

export default function ImportIndexListItem({
    indexDefinition,
    toggleIndexName,
    selectedIndexNames,
    disabledReason,
}: ImportIndexListItemProps) {
    return (
        <ListGroupItem key={indexDefinition.Name} disabled={!!disabledReason}>
            <Label className="d-flex gap-1 align-items-center m-0 text-truncate">
                <div className="d-flex gap-1 align-items-center w-100">
                    {!disabledReason && (
                        <Checkbox
                            toggleSelection={() => toggleIndexName(indexDefinition.Name)}
                            selected={selectedIndexNames.includes(indexDefinition.Name)}
                            size="md"
                            color="primary"
                        />
                    )}
                    <IndexToMigrateTitle index={indexDefinition} disabledReason={disabledReason} />
                </div>
            </Label>
        </ListGroupItem>
    );
}
