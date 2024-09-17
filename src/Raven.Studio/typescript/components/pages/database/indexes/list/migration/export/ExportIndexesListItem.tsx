import { IndexSharedInfo } from "components/models/indexes";
import IndexToMigrateTitle from "components/pages/database/indexes/list/migration/common/IndexToMigrateTitle";
import React from "react";
import { ListGroupItem } from "reactstrap";

interface ExportIndexesListItemProps {
    index: IndexSharedInfo;
    disabledReason?: string;
}

export default function ExportIndexesListItem({ index, disabledReason }: ExportIndexesListItemProps) {
    return (
        <ListGroupItem key={index.name} disabled={!!disabledReason}>
            <IndexToMigrateTitle index={index} disabledReason={disabledReason} />
        </ListGroupItem>
    );
}
