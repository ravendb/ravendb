import {
    millisecondsInDay,
    millisecondsInHour,
    millisecondsInMinute,
    millisecondsInSecond,
    millisecondsInWeek,
    secondsInDay,
    secondsInHour,
    secondsInMinute,
    secondsInWeek,
} from "date-fns/constants";

// A multiplication table for durations, e.g. `14 * MS_IN.day`. It stops at week on purpose:
// months and years have no fixed length, so they belong to date arithmetic (date-fns, date-period.ts).
export const SECONDS_IN = {
    minute: secondsInMinute,
    hour: secondsInHour,
    day: secondsInDay,
    week: secondsInWeek,
} as const;

export const MS_IN = {
    second: millisecondsInSecond,
    minute: millisecondsInMinute,
    hour: millisecondsInHour,
    day: millisecondsInDay,
    week: millisecondsInWeek,
} as const;
