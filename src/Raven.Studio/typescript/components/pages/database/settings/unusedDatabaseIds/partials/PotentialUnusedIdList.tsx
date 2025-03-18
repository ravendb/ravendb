import { CounterBadge } from "components/common/CounterBadge";
import PotentialUnusedId from "components/pages/database/settings/unusedDatabaseIds/partials/PotentialUnusedId";
import { UnusedIdsActions } from "components/pages/database/settings/unusedDatabaseIds/useUnusedDatabaseIds";
import React from "react";
import Button from "react-bootstrap/Button";

interface PotentialUnusedIdListProps {
    potentialUnusedId: string[];
    unusedIds: string[];
    unusedIdsActions: UnusedIdsActions;
}

export default function PotentialUnusedIdList(props: PotentialUnusedIdListProps) {
    const { potentialUnusedId, unusedIds, unusedIdsActions } = props;

    if (potentialUnusedId.length === 0) {
        return null;
    }

    return (
        <>
            <div className="d-flex gap-1 align-items-center justify-content-between mt-3">
                <div className="d-flex gap-1">
                    <h4 className="mb-0">IDs that may be added to the list</h4>
                    <CounterBadge count={potentialUnusedId.length} />
                </div>
                <Button
                    variant="link"
                    size="xs"
                    onClick={unusedIdsActions.addAllPotentialUnusedIds}
                    className="p-0"
                    title="Add all potential unused IDs to the list"
                >
                    Add all
                </Button>
            </div>
            <div className="d-flex flex-wrap gap-1 mt-1">
                {potentialUnusedId.map((id) => (
                    <PotentialUnusedId
                        key={id}
                        id={id}
                        addUnusedId={() => unusedIdsActions.add(id)}
                        isAdded={unusedIds.includes(id)}
                    />
                ))}
            </div>
        </>
    );
}
