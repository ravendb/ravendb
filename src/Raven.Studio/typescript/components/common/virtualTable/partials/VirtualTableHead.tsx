import { Column, Table as TanstackTable, flexRender } from "@tanstack/react-table";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, Input, Label, Button } from "reactstrap";
import { HStack } from "components/common/HStack";
import "./VirtualTableHead.scss";
import { todo } from "common/developmentHelper";

interface VirtualTableHeadProps<T> {
    table: TanstackTable<T>;
}

export default function VirtualTableHead<T>({ table }: VirtualTableHeadProps<T>) {
    return (
        <thead>
            {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id} className="d-flex">
                    {headerGroup.headers.map((header) => (
                        <th
                            key={header.id}
                            className="position-relative align-content-center"
                            style={{ width: header.getSize() }}
                        >
                            <div
                                className="position-relative d-flex align-items-center justify-content-between"
                                title={getHeaderTitle(header.column)}
                            >
                                {flexRender(header.column.columnDef.header, header.getContext())}

                                <ColumnSettings column={header.column}></ColumnSettings>
                            </div>
                            {header.column.getCanResize() && (
                                <div
                                    className={classNames("resizer", {
                                        "is-resizing": header.column.getIsResizing(),
                                    })}
                                    onMouseDown={header.getResizeHandler()}
                                    onTouchStart={header.getResizeHandler()}
                                ></div>
                            )}
                        </th>
                    ))}
                </tr>
            ))}
        </thead>
    );
}

function getHeaderTitle<T>(column: Column<T, unknown>): string {
    const { columnDef } = column;

    if (typeof columnDef.header === "string") {
        return columnDef.header;
    }

    if ("accessorKey" in columnDef && typeof columnDef.accessorKey === "string") {
        return columnDef.accessorKey;
    }

    return columnDef.id;
}

function ColumnSettings<T>({ column }: { column: Column<T, unknown> }) {
    const [localFilter, setLocalFilter] = useState("");

    const debouncedSetFilter = useMemo(
        () => _.debounce((value: string) => column.setFilterValue(value), 500),
        [column]
    );

    const handleFilterChange = (value: string) => {
        setLocalFilter(value);
        debouncedSetFilter(value);
    };

    todo("BugFix", "Damian", "Fix logic to handle double click properly");

    const handleSort = (direction: "asc" | "desc") => {
        const currentSort = column.getIsSorted();

        switch (direction) {
            case "asc":
                column.toggleSorting(currentSort === "asc" ? undefined : false);
                break;
            case "desc":
                column.toggleSorting(currentSort === "desc" ? undefined : true);
                break;
        }
    };

    if (document.querySelector("#page-host") == null) {
        return null;
    }

    if (!column.getCanSort() && !column.getCanFilter()) {
        return null;
    }

    return (
        <UncontrolledDropdown>
            <HStack>
                {column.getCanSort() && (
                    <div className="sorting-controls">
                        <Button
                            color="link"
                            onClick={() => handleSort("asc")}
                            title="Sort A to Z"
                            className={classNames(column.getIsSorted() === "asc" && "active-sorting")}
                        >
                            <Icon icon="arrow-thin-top" margin="m-0" />
                        </Button>
                        <Button
                            color="link"
                            onClick={() => handleSort("desc")}
                            title="Sort Z to A"
                            className={classNames(column.getIsSorted() === "desc" && "active-sorting")}
                        >
                            <Icon icon="arrow-thin-bottom" margin="m-0" />
                        </Button>
                    </div>
                )}
                <DropdownToggle
                    title="Column settings"
                    color="link"
                    className={classNames(localFilter ? "active-filtering" : "link-muted", "filtering-controls")}
                    size="sm"
                >
                    <Icon icon="filter" margin="m-0" />
                </DropdownToggle>
            </HStack>
            <DropdownMenu container="page-host">
                {column.getCanFilter() && (
                    <div className="px-3 pb-2">
                        <Label className="small-label">Filter column</Label>
                        <div className="clearable-input">
                            <Input
                                type="text"
                                placeholder="Search..."
                                value={localFilter}
                                onChange={(e) => handleFilterChange(e.target.value)}
                                className="pe-4"
                            />
                            {localFilter && (
                                <div className="clear-button">
                                    <Button color="secondary" size="sm" onClick={() => handleFilterChange("")}>
                                        <Icon icon="clear" margin="m-0" />
                                    </Button>
                                </div>
                            )}
                        </div>
                    </div>
                )}
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
