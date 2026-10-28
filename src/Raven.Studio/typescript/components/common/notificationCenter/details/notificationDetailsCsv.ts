import { Column, Table } from "@tanstack/react-table";
import moment from "moment";
import { toCsv } from "common/csvUtils";

export function tableToCsv<T>(table: Table<T>): string {
    const dataColumns = table.getVisibleLeafColumns().filter((column) => column.accessorFn);

    const headerRow = dataColumns.map((column) => column.columnDef.meta?.exportHeader ?? getHeaderText(column));
    const dataRows = table.getRowModel().rows.map((row) => dataColumns.map((column) => row.getValue(column.id)));

    return toCsv([headerRow, ...dataRows]);
}

export function getCsvFileName(databaseName: string, title: string, date = moment()): string {
    const parts = [databaseName, title, date.format("YYYY-MM-DD-HHmm")];
    return parts.map(toFileNamePart).filter(Boolean).join("-") + ".csv";
}

function getHeaderText<T>(column: Column<T, unknown>): string {
    const { header } = column.columnDef;
    return typeof header === "string" ? header : column.id;
}

function toFileNamePart(value: string): string {
    return (value ?? "")
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, "-")
        .replace(/^-|-$/g, "");
}
