import { Getter } from "@tanstack/react-table";
import React, { useMemo } from "react";
import moment from "moment";
import genUtils from "common/generalUtils";
import CellValue from "components/common/virtualTable/cells/CellValue";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

interface CellDateWithRelativeTimeProps {
    value: unknown;
}

function parseDate(value: unknown): Date | null {
    if (value instanceof Date) {
        return isNaN(value.getTime()) ? null : value;
    }
    if (typeof value === "string" || typeof value === "number") {
        const parsed = new Date(value);
        return isNaN(parsed.getTime()) ? null : parsed;
    }
    return null;
}

export default function CellDateWithRelativeTime({ value }: CellDateWithRelativeTimeProps) {
    const dateValue = useMemo(() => parseDate(value), [value]);

    if (!dateValue) {
        return <CellValue value="-" />;
    }

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <b>UTC:</b> {moment(dateValue).utc().format(genUtils.dateFormat)}
                </>
            }
        >
            <small className="vstack cell-value value-string">
                <span>{moment(dateValue).format(genUtils.dateFormat)}</span>
                <small>{moment(dateValue).fromNow()}</small>
            </small>
        </PopoverWithHoverWrapper>
    );
}

export function CellDateWithRelativeTimeWrapper({ getValue }: { getValue: Getter<unknown> }) {
    return <CellDateWithRelativeTime value={getValue()} />;
}
