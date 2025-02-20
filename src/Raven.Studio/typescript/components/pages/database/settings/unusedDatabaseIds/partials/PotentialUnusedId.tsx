import React from "react";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import Button from "react-bootstrap/Button";

interface PotentialUnusedIdProps {
    id: string;
    addUnusedId: () => void;
    isAdded: boolean;
}

export default function PotentialUnusedId(props: PotentialUnusedIdProps) {
    const { id, addUnusedId, isAdded } = props;

    return (
        <ConditionalPopover
            conditions={{
                isActive: isAdded,
                message: "This ID has already been added to the list",
            }}
            popoverPlacement="top"
        >
            <Button
                className="rounded-pill"
                onClick={addUnusedId}
                title="Copy ID"
                variant="outline-secondary"
                disabled={isAdded}
            >
                <strong>{id}</strong>
            </Button>
        </ConditionalPopover>
    );
}
