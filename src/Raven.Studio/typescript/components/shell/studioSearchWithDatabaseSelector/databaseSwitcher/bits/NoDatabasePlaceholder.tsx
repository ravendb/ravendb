import DatabaseIcon from "./DatabaseIcon";
import React from "react";

export default function NoDatabasePlaceholder() {
    return (
        <span>
            <DatabaseIcon databaseName={null} />
            No database selected
        </span>
    );
}
