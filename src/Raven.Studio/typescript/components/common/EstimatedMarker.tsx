import React from "react";

export function EstimatedMarker() {
    return (
        <small
            className="text-muted"
            title="Estimated value. The exact count takes too long to calculate and will be skipped for performance."
        >
            (~)
        </small>
    );
}
