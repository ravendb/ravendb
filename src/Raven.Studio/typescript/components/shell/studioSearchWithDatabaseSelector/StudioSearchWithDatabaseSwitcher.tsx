import DatabaseSwitcher from "./databaseSwitcher/DatabaseSwitcher";
import StudioSearch from "./studioSearch/StudioSearch";
import React from "react";
import { InputGroup } from "reactstrap";

export default function StudioSearchWithDatabaseSwitcher() {
    return (
        <InputGroup>
            <StudioSearch />
            <DatabaseSwitcher />
        </InputGroup>
    );
}
