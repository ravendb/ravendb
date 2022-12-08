import React from "react";
import { Badge, Card, CardBody } from "reactstrap";

export function DeletionInProgress(props: { nodeTag: string }) {
    const { nodeTag } = props;
    return (
        <Card>
            <CardBody>
                Node: {nodeTag}
                <Badge color="warning">DELETING</Badge>
                <h3 className="pulse text-warning" title="Deletion in progress">
                    <i className="icon-trash" /> Node: {nodeTag}
                </h3>
            </CardBody>
        </Card>
    );
}
