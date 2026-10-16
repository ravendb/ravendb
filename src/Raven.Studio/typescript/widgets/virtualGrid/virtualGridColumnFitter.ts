export interface measuredColumn {
    header: number;
    content: number;
}

const shortContentLimit = 220;
const longTextMinimum = 120;

const sum = (values: number[]) => values.reduce((total, value) => total + value, 0);

export function fitColumnWidths(columns: measuredColumn[], availableWidth: number): number[] {
    const widestIndex = columns.reduce(
        (widest, column, index) => (column.content >= columns[widest].content ? index : widest),
        0
    );
    const isLongText = columns.map((column, index) => index === widestIndex || column.content > shortContentLimit);
    const minimumWidth = (index: number) => Math.max(columns[index].header, longTextMinimum);

    const widths = columns.map((column, index) => (isLongText[index] ? minimumWidth(index) : Math.max(column.header, column.content)));

    let spare = availableWidth - sum(widths);
    let growing = columns.map((_, index) => index).filter((index) => isLongText[index] && columns[index].content > widths[index]);

    while (spare > 0 && growing.length > 0) {
        const totalContent = sum(growing.map((index) => columns[index].content));
        const share = (index: number) => (spare * columns[index].content) / totalContent;
        const satisfied = growing.find((index) => widths[index] + share(index) >= columns[index].content);

        if (satisfied === undefined) {
            growing.forEach((index) => (widths[index] += Math.floor(share(index))));
            break;
        }

        spare -= columns[satisfied].content - widths[satisfied];
        widths[satisfied] = columns[satisfied].content;
        growing = growing.filter((index) => index !== satisfied);
    }

    widths[widestIndex] += Math.max(0, availableWidth - sum(widths));
    return widths;
}
