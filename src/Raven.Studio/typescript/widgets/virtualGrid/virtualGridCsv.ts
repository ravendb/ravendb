import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import { toCsv } from "common/csvUtils";

export function gridToCsv<T extends object>(columns: virtualColumn[], items: T[]): string {
    const exportedColumns = columns.filter((column): column is textColumn<T> => column instanceof textColumn);

    return toCsv([
        exportedColumns.map((column) => column.opts.exportHeader ?? column.headerAsText),
        ...items.map((item) =>
            exportedColumns.map((column) => (column.opts.exportValue ? column.opts.exportValue(item) : column.getCellValue(item)))
        ),
    ]);
}
