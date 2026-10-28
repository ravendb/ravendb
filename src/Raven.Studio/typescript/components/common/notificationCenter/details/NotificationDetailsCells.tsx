import { CellContext, ColumnDef, FilterFn, Getter } from "@tanstack/react-table";
import moment from "moment";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import CellValue from "components/common/virtualTable/cells/CellValue";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import genUtils from "common/generalUtils";

type ValueCellProps = Pick<CellContext<unknown, unknown>, "getValue">;

export function CellDetailsValue({ getValue }: ValueCellProps) {
    const value = getValue();

    if (value == null || value === "") {
        return <CellNotAvailable />;
    }

    return <CellWithCopyWrapper getValue={getValue} />;
}

export const columnSizes = {
    date: 250,
    bytes: 150,
    duration: 150,
};

export function CellDate<T>({ getValue, cell }: Pick<CellContext<T, unknown>, "getValue" | "cell">) {
    const utcDate = getValue<string>();

    if (!utcDate) {
        return <CellNotAvailable />;
    }

    const getDate = (() => moment.utc(utcDate).toDate()) as Getter<unknown>;

    return <DateFormatterCell cell={cell} getValue={getDate} />;
}

const dateFilter: FilterFn<any> = (row, columnId, filterValue: string) => {
    const utcDate = row.getValue<string>(columnId);
    if (!utcDate) {
        return false;
    }

    const lowercaseFilter = filterValue.toLowerCase();
    return (
        genUtils.formatUtcDateAsLocal(utcDate).toLowerCase().includes(lowercaseFilter) ||
        utcDate.toLowerCase().includes(lowercaseFilter)
    );
};

export const dateColumnOptions = {
    cell: CellDate,
    filterFn: dateFilter,
    size: columnSizes.date,
    meta: { filterPlaceholder: "e.g. October 20th" },
};

export function CellBytes({ getValue }: ValueCellProps) {
    const value = getValue() as number;

    if (value == null) {
        return <CellNotAvailable />;
    }

    return <CellWithFormattedValue value={value} formattedValue={genUtils.formatBytesToSize(value)} />;
}

export function CellWithFormattedValue({ value, formattedValue }: { value: unknown; formattedValue: string }) {
    return (
        <CellWithCopy value={value}>
            <CellValue value={formattedValue} />
        </CellWithCopy>
    );
}

export function CellNotAvailable() {
    return <span className="text-muted">n/a</span>;
}

export function previewColumn<T>(onPreview: (item: T) => void): ColumnDef<T> {
    return {
        id: "Preview",
        size: 44,
        enableSorting: false,
        enableColumnFilter: false,
        enableResizing: false,
        cell: ({ row }) => (
            <Button
                variant="link"
                size="sm"
                className="p-0"
                title="Show details below the table"
                onClick={() => onPreview(row.original)}
            >
                <Icon icon="preview" margin="m-0" />
            </Button>
        ),
    };
}
