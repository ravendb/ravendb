import moment from "moment";
import { NotificationDetailsActions } from "../notificationDetailsProvider";

export const storyActions: NotificationDetailsActions = {
    close: () => undefined,
    dismiss: () => undefined,
    postpone: () => Promise.resolve(),
};

export function storyDate(minutesAgo: number): string {
    return moment.utc("2026-10-20T10:00:00Z").subtract(minutesAgo, "minutes").toISOString();
}

export function times<T>(count: number, create: (index: number) => T): T[] {
    return Array.from({ length: count }, (_, index) => create(index));
}
