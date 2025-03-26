import { CellContext } from "@tanstack/react-table";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import React, { useMemo } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import moment from "moment";

type CellContextSubset<TData, TValue> = Pick<CellContext<TData, TValue>, "cell" | "getValue">;

export interface DateFormatterCellProps<TData, TValue> extends CellContextSubset<TData, TValue> {
    cellClassName?: string;
    displayFormat?: string;
    showTooltip?: boolean;
}

export function DateFormatterCell<TData, TValue>({
    getValue,
    cellClassName,
    displayFormat,
    showTooltip = true,
}: DateFormatterCellProps<TData, TValue>) {
    const rawValue = getValue();

    const dateValue = useMemo(() => {
        if (rawValue instanceof Date) {
            return rawValue;
        }
        if (typeof rawValue === "string" || typeof rawValue === "number") {
            const parsed = new Date(rawValue);
            return isNaN(parsed.getTime()) ? null : parsed;
        }
        return null;
    }, [rawValue]);

    const formattedDate = useMemo(() => {
        if (!dateValue) {
            return "";
        }
        return displayFormat ? moment(dateValue).format(displayFormat) : String(rawValue);
    }, [dateValue, displayFormat, rawValue]);

    if (!dateValue) {
        return <CellValueWrapper className={cellClassName} getValue={getValue} />;
    }

    if (!showTooltip) {
        return <CellValueWrapper className={cellClassName} getValue={getValue} />;
    }

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <div className="index-errors-details-tooltip__container">
                        <b>UTC: </b>
                        <time className="index-errors-details-tooltip__date">
                            {moment.utc(dateValue).toISOString()}
                        </time>
                    </div>
                    <div className="index-errors-details-tooltip__container">
                        <b>Relative: </b>
                        <time>{moment(dateValue).fromNow()}</time>
                    </div>
                </>
            }
        >
            <CellValue value={formattedDate} className={cellClassName} />
        </PopoverWithHoverWrapper>
    );
}

export default DateFormatterCell;
