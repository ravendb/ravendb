import "./TableDisplaySettings.scss";
import { Table as TanstackTable } from "@tanstack/react-table";
import { Checkbox } from "components/common/Checkbox";
import { Button, Dropdown, DropdownMenu, DropdownToggle } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useTableDisplaySettings } from "components/common/virtualTable/commonComponents/columnsSelect/useTableDisplaySettings";
import { ClassNameProps } from "components/models/common";
import classNames from "classnames";

import { todo } from "common/developmentHelper";
todo(
    "Feature",
    "Damian",
    "Add custom column and reorder with dnd",
    "https://issues.hibernatingrhinos.com/issue/RavenDB-22509"
);

interface TableColumnsSelectProps<T> extends ClassNameProps {
    table: TanstackTable<T>;
}

export default function TableDisplaySettings<T>({ table, className }: TableColumnsSelectProps<T>) {
    const { isDropdownOpen, availableColumnsIds, selectionState, getIsColumnSelected, handlers } =
        useTableDisplaySettings(table);

    return (
        <div className={classNames("table-display-settings", className)}>
            <Dropdown isOpen={isDropdownOpen} toggle={handlers.handleToggleDropdown}>
                <DropdownToggle caret>
                    <Icon icon="table" /> Display settings
                </DropdownToggle>
                <DropdownMenu>
                    <div className="px-3 py-1 d-flex flex-row gap-1">
                        <Checkbox
                            selected={selectionState === "AllSelected"}
                            toggleSelection={handlers.handleToggleAll}
                            indeterminate={selectionState === "SomeSelected"}
                            color="primary"
                            title="Select all"
                        >
                            Select all
                        </Checkbox>
                    </div>
                    <hr className="m-0" />
                    <div className="m-0 well">
                        <div className="d-flex flex-column column-list">
                            {availableColumnsIds.map((id) => (
                                <div key={id} className="d-flex flex-row gap-1 column-list-item">
                                    <Checkbox
                                        selected={getIsColumnSelected(id)}
                                        toggleSelection={() => handlers.handleToggleOne(id)}
                                        title={`Select ${id}`}
                                    >
                                        {id}
                                    </Checkbox>
                                </div>
                            ))}
                        </div>
                    </div>
                    <hr className="m-0" />
                    <div className="d-flex gap-1 px-3 pt-3 pb-2">
                        <Button type="button" title="Reset to default" onClick={handlers.handleReset}>
                            <Icon icon="reset" />
                            Reset to default
                        </Button>
                        <Button type="button" className="ms-3" title="Close" onClick={handlers.handleCloseDropdown}>
                            Close
                        </Button>
                        <Button type="button" color="success" title="Apply changes" onClick={handlers.handleSave}>
                            <Icon icon="save" />
                            Apply
                        </Button>
                    </div>
                    {/* TODO RavenDB-22509 
                    {isAddCustomColumnActive && (
                        <div className="d-flex gap-1 px-3 pt-3">
                            <Input placeholder="Binding" defaultValue="this." />
                            <Input placeholder="Alias" />
                        </div>
                    )}
                    <div className="d-flex gap-1 px-3 pt-3 pb-2">
                        {isAddCustomColumnActive ? (
                            <>
                                <FlexGrow />
                                <Button color="secondary" onClick={toggleIsAddCustomColumnActive} title="Cancel">
                                    Cancel
                                </Button>
                                <Button
                                    color="success"
                                    onClick={toggleIsAddCustomColumnActive}
                                    title="Save custom column"
                                >
                                    <Icon icon="save" /> Save custom column
                                </Button>
                            </>
                        ) : (
                            <>
                                <Button
                                    color="primary"
                                    onClick={toggleIsAddCustomColumnActive}
                                    title="Add custom column"
                                >
                                    <Icon icon="plus" />
                                    Add custom column
                                </Button>
                                <Button title="Reset to default">
                                    <Icon icon="reset" />
                                    Reset to default
                                </Button>
                                <Button className="ms-3" title="Close">
                                    Close
                                </Button>
                                <Button color="success" title="Apply changes">
                                    <Icon icon="save" />
                                    Apply
                                </Button>
                            </>
                        )}
                    </div> */}
                </DropdownMenu>
            </Dropdown>
        </div>
    );
}
