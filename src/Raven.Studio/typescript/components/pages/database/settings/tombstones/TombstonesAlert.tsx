import React from "react";
import RichAlert from "components/common/RichAlert";

export default function TombstonesAlert() {
    return (
        <RichAlert variant="info" className="mb-0">
            <p>
                By default, tombstones cleanup is scheduled to be carried out by the server every 5 minutes, unless
                configured otherwise.
            </p>
            <p className="mb-0">
                Upon clicking <strong>Force Cleanup</strong>, the action will be executed immediately - any tombstone
                that can be removed will be deleted.
            </p>
        </RichAlert>
    );
}
