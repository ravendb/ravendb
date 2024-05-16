import { Getter } from "@tanstack/react-table";
import classNames from "classnames";

interface CellValueProps {
    value: unknown;
    title?: string;
}

export default function CellValue({ value, title }: CellValueProps) {
    if (value === undefined) {
        return null;
    }

    if (value === null) {
        return <span className="cell-value value-null">null</span>;
    }

    if (typeof value === "object") {
        return (
            <span className="cell-value">
                {Array.isArray(value) ? (
                    <>
                        <span className="value-object">[...]</span>
                        <sup>{value.length}</sup>
                    </>
                ) : (
                    <>
                        <span className="value-object"> {"{...}"}</span>
                        <sup>{Object.keys(value).length}</sup>
                    </>
                )}
            </span>
        );
    }

    if (typeof value === "number") {
        return (
            <span title={title} className="cell-value value-number">
                {value.toLocaleString()}
            </span>
        );
    }

    return (
        <span title={title} className={classNames("cell-value", `value-${typeof value}`)}>
            {String(value)}
        </span>
    );
}

export function CellValueWrapper({ getValue }: { getValue: Getter<unknown> }) {
    return <CellValue value={getValue()} />;
}
