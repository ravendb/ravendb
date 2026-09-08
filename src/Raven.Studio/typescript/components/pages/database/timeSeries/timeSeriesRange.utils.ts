import moment from "moment";

export type FilterTimezone = "local" | "utc";

export const FULL_FORMAT = "YYYY-MM-DD HH:mm:ss.SSS";

export function wallOf(instant: moment.Moment, tz: FilterTimezone): moment.Moment {
    return tz === "utc" ? instant.clone().utc() : instant.clone().local();
}

export function zoneLabel(tz: FilterTimezone): string {
    return tz === "utc" ? "UTC" : "Local";
}
