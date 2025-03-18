import { Column, SortDirection } from "@tanstack/react-table";
import classNames from "classnames";
import { HStack } from "components/common/utilities/HStack";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import Form from "react-bootstrap/Form";

import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { FormLabel } from "components/common/Form";

export default function VirtualTableColumnSettings<T>({ column }: { column: Column<T, unknown> }) {
    const [localFilter, setLocalFilter] = useState("");

    const debouncedSetFilter = useMemo(
        () => _.debounce((value: string) => column.setFilterValue(value), 300),
        [column]
    );

    const handleFilterChange = (value: string) => {
        setLocalFilter(value);
        debouncedSetFilter(value);
    };

    const handleSort = (direction: SortDirection) => {
        if (column.getIsSorted() === direction) {
            column.clearSorting();
            return;
        }

        if (direction === "asc") {
            column.toggleSorting(false);
        }

        if (direction === "desc") {
            column.toggleSorting(true);
        }
    };

    if (document.querySelector("#page-host") == null) {
        return null;
    }

    if (!column.getCanSort() && !column.getCanFilter()) {
        return null;
    }

    return (
        <HStack>
            {column.getCanSort() && (
                <div className="sorting-controls">
                    <Button
                        variant="link"
                        onClick={() => handleSort("asc")}
                        title="Sort A to Z"
                        className={classNames(column.getIsSorted() === "asc" && "active-sorting")}
                    >
                        <Icon icon="arrow-thin-top" margin="m-0" />
                    </Button>
                    <Button
                        variant="link"
                        onClick={() => handleSort("desc")}
                        title="Sort Z to A"
                        className={classNames(column.getIsSorted() === "desc" && "active-sorting")}
                    >
                        <Icon icon="arrow-thin-bottom" margin="m-0" />
                    </Button>
                </div>
            )}
            {column.getCanFilter() && (
                <Dropdown>
                    <Dropdown.Toggle
                        title="Column settings"
                        as={CustomDropdownToggle}
                        isCaretHidden
                        variant="link"
                        className={classNames(
                            column.getFilterValue() ? "active-filtering" : "link-muted",
                            "filtering-controls"
                        )}
                        size="sm"
                    >
                        <Icon icon="filter" margin="m-0" />
                    </Dropdown.Toggle>
                    <Dropdown.Menu renderOnMount popperConfig={{ strategy: "fixed" }}>
                        <div className="px-3 pb-2">
                            <FormLabel className="small-label">Filter column</FormLabel>
                            <div className="clearable-input">
                                <Form.Control
                                    type="text"
                                    placeholder="Search..."
                                    value={localFilter}
                                    onChange={(e) => handleFilterChange(e.target.value)}
                                    className="pe-4"
                                />
                                {localFilter && (
                                    <div className="clear-button">
                                        <Button variant="secondary" size="sm" onClick={() => handleFilterChange("")}>
                                            <Icon icon="clear" margin="m-0" />
                                        </Button>
                                    </div>
                                )}
                            </div>
                        </div>
                    </Dropdown.Menu>
                </Dropdown>
            )}
        </HStack>
    );
}
