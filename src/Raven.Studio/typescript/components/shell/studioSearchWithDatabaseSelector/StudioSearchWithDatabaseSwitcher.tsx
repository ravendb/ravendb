import menu from "common/shell/menu";
import DatabaseSwitcher from "./databaseSwitcher/DatabaseSwitcher";
import StudioSearch from "./studioSearch/StudioSearch";
import React from "react";
import { InputGroup } from "reactstrap";

export default function StudioSearchWithDatabaseSwitcher(props: { mainMenu: menu }) {
    return (
        <InputGroup>
            <DatabaseSwitcher />
            <StudioSearch mainMenu={props.mainMenu} />
        </InputGroup>
    );
}
