import { Checkbox } from "components/common/Checkbox";
import IndexToMigrateTitle from "components/pages/database/indexes/list/migration/common/IndexToMigrateTitle";
import React from "react";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import { FormLabel } from "components/common/Form";

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
            <FormLabel className="d-flex gap-1 align-items-center m-0 text-truncate">
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
            </FormLabel>
        </ListGroupItem>
    );
}
