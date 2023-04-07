import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    switch (dbAccess) {
        case "DatabaseAdmin":
            return (
                <span title="Admin Access">
                    <i className="icon-access-admin"/>
                </span>
            );
        case "DatabaseReadWrite":
            return (
                <span title="Read/Write Access">
                    <i className="icon-access-read-write"/>
                </span>
            );
        case "DatabaseRead":
            return (
                <span title="Read-only Access">
                    <i className="icon-access-read"/>
                </span>
            );
        default:
            assertUnreachable(dbAccess);
    }
}
