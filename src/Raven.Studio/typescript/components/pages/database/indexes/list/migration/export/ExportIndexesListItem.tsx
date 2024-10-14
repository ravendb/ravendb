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
            <div className="d-flex gap-1 align-items-center m-0 text-truncate">
                <IndexToMigrateTitle index={index} disabledReason={disabledReason} />
            </div>
        </ListGroupItem>
    );
}
