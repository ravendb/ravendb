import { Badge } from "reactstrap";
import React from "react";

export default function DisabledBadge({ isDisabled }: { isDisabled: boolean }) {
    if (!isDisabled) {
        return null;
    }

    return (
        <Badge className="ms-2" pill>
            Disabled
        </Badge>
    );
}
