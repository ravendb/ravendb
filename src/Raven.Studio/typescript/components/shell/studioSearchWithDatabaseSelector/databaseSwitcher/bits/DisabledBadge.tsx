import Badge from "react-bootstrap/Badge";
import React from "react";

export default function DisabledBadge({ isDisabled }: { isDisabled: boolean }) {
    if (!isDisabled) {
        return null;
    }

    return (
        <Badge className="ms-2" pill bg="secondary">
            Disabled
        </Badge>
    );
}
