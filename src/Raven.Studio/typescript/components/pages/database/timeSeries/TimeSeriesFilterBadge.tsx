import React from "react";
import moment from "moment";
import { FilterTimezone, wallOf, zoneLabel } from "./timeSeriesRange.utils";
import "./TimeSeriesFilterBadge.scss";

const BADGE_FORMAT = "M/D/YYYY HH:mm";

interface TimeSeriesFilterBadgeProps {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    // Zone the grid is displaying timestamps in. The bounds are absolute instants, so
    // without this the badge always renders browser-local and can contradict a UTC grid.
    timezone?: FilterTimezone;
    onEdit: () => void;
    onClear: () => void;
}

function formatFilter(
    startDate: moment.Moment | null,
    endDate: moment.Moment | null,
    timezone: FilterTimezone
): string | null {
    const fmt = (d: moment.Moment) => wallOf(d, timezone).format(BADGE_FORMAT);
    const zone = zoneLabel(timezone);

    if (startDate && endDate) {
        return `Between ${fmt(startDate)} - ${fmt(endDate)} (${zone})`;
    }
    if (endDate) {
        return `Before ${fmt(endDate)} (${zone})`;
    }
    if (startDate) {
        return `After ${fmt(startDate)} (${zone})`;
    }
    return null;
}

export default function TimeSeriesFilterBadge({
    startDate,
    endDate,
    timezone = "local",
    onEdit,
    onClear,
}: TimeSeriesFilterBadgeProps) {
    const text = formatFilter(startDate, endDate, timezone);

    if (!text) {
        return null;
    }

    // A bootstrap-3 split button (must stay outside a .bs5 scope so the legacy .btn styling
    // resolves): left segment opens the filter dialog, right segment clears it.
    return (
        <div className="btn-group time-series-filter-badge">
            <button type="button" className="btn" title="Edit filter" onClick={onEdit}>
                <i className="icon-calendar" />
                <span className="margin-left margin-left-xs">{text}</span>
            </button>
            <button
                type="button"
                className="btn time-series-filter-badge__clear"
                title="Clear filter"
                onClick={onClear}
            >
                <i className="icon-cancel" />
            </button>
        </div>
    );
}
