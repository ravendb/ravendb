import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import React from "react";

export function AccessIcon(props: { dbAccess: databaseAccessLevel }) {
    const { dbAccess } = props;
    switch (dbAccess) {
        case "DatabaseAdmin":
            return <Icon icon="access-admin" title="Admin Access"></Icon>;
        case "DatabaseReadWrite":
            return <Icon icon="access-read-write" title="Read/Write Access"></Icon>;
        case "DatabaseRead":
            return <Icon icon="access-read" title="Read-only Access"></Icon>;
        default:
            assertUnreachable(dbAccess);
    }
}
