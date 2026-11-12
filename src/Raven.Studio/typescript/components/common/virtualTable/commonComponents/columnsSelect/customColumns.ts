import genUtils from "common/generalUtils";

export interface CustomColumnDefinition {
    id: string;
    header: string;
    expression: string;
}

export function createCustomColumnId() {
    return `custom-column-${genUtils.generateUUID()}`;
}

type CompiledExpression = (this: unknown) => unknown;

function compileExpression(expression: string): CompiledExpression {
    return new Function(`return (${expression})`) as CompiledExpression;
}

export function getCustomColumnExpressionError(expression: string): string | null {
    try {
        compileExpression(expression);
        return null;
    } catch (e) {
        return (e as Error).message;
    }
}

export function createCustomColumnAccessor(expression: string): (item: unknown) => unknown {
    let evaluate: CompiledExpression;

    try {
        evaluate = compileExpression(expression);
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

export function getCustomColumnProperties(expression: string): string[] {
    const properties = new Set<string>();
    const propertyRegex = /this(?:\.(\w+)|\[\s*(["'`])(.+?)\2\s*\])/g;

    let match: RegExpExecArray;
    while ((match = propertyRegex.exec(expression)) !== null) {
        properties.add(match[1] ?? match[3]);
    }

    return Array.from(properties);
}
