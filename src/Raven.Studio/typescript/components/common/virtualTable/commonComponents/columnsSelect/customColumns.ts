import { RowData } from "@tanstack/react-table";

// a column defined by the user with a JavaScript expression evaluated against every row, e.g. this.ShipTo.City
export interface CustomColumnDefinition {
    id: string;
    header: string;
    expression: string;
}

declare module "@tanstack/react-table" {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    interface ColumnMeta<TData extends RowData, TValue> {
        customColumn?: CustomColumnDefinition;
    }
}

export function createCustomColumnId() {
    return `custom-column-${crypto.randomUUID()}`;
}

export function getCustomColumnExpressionError(expression: string): string | null {
    try {
        new Function(`return (${expression})`);
        return null;
    } catch (e) {
        return (e as Error).message;
    }
}

export function createCustomColumnAccessor(expression: string): (item: unknown) => unknown {
    let evaluate: (this: unknown) => unknown;

    try {
        evaluate = new Function(`return (${expression})`) as typeof evaluate;
    } catch (e) {
        return () => `Unable to parse the expression: ${(e as Error).message}`;
    }

    return (item) => {
        try {
            return evaluate.call(item);
        } catch (e) {
            return `Unable to evaluate the expression: ${(e as Error).message}`;
        }
    };
}

// the expression accesses the row as `this`, so this.FirstName + this.Address.City gives [FirstName, Address]
export function getCustomColumnProperties(expression: string): string[] {
    const properties = new Set<string>();
    const propertyRegex = /this\.(\w+)/g;

    let match: RegExpExecArray;
    while ((match = propertyRegex.exec(expression)) !== null) {
        properties.add(match[1]);
    }

    return Array.from(properties);
}
