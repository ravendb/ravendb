import React from "react";
import { Badge } from "reactstrap";
import { EmptySet } from "./EmptySet";
import { Icon } from "./Icon";

export default function FeatureNotAvailable() {
    return (
        <div>
            <EmptySet icon="disabled" color="warning">
                <div className="vstack gap-3">
                    <span>
                        <Badge pill color="faded-warning">
                            Feature not available
                        </Badge>
                    </span>
                    <span>
                        Import documents from a CSV file into a collection is not available for{" "}
                        <Icon icon="sharding" color="shard" margin="m-0" /> sharded databases
                    </span>
                </div>
            </EmptySet>
        </div>
    );
}
