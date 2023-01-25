import { DatabaseGroupActions, DatabaseGroupItem, DatabaseGroupNode } from "components/common/DatabaseGroup";
import React from "react";
import { Spinner } from "reactstrap";

export function DeletionInProgress(props: { nodeTag: string }) {
    const { nodeTag } = props;
    return (
        <DatabaseGroupItem className="item-disabled">
            <DatabaseGroupNode>{nodeTag}</DatabaseGroupNode>
            <DatabaseGroupActions>
                <div className="pulse text-progress" title="Deletion in progress">
                    <Spinner size="sm" className="me-1" /> Deleting
                </div>
            </DatabaseGroupActions>
        </DatabaseGroupItem>
    );
}
