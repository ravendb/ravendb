import { ComponentType } from "react";
import abstractNotification from "common/notifications/models/abstractNotification";
import alert from "common/notifications/models/alert";
import performanceHint from "common/notifications/models/performanceHint";
import operation from "common/notifications/models/operation";
import type notificationCenter from "common/notifications/notificationCenter";

type AlertReason = Raven.Server.NotificationCenter.Notifications.AlertReason;
type PerformanceHintReason = Raven.Server.NotificationCenter.Notifications.PerformanceHintReason;
type OperationType = Raven.Server.Documents.Operations.OperationType;

export interface NotificationDetailsActions {
    close: () => void;
    dismiss: () => void;
    postpone: (timeInSeconds: number) => Promise<void>;
}

export interface NotificationDetailsProps<T extends abstractNotification> {
    notification: T;
    actions: NotificationDetailsActions;
}

export function createDetailsProvider<T extends abstractNotification>(
    supportsDetailsFor: (notification: abstractNotification) => notification is T,
    component: ComponentType<NotificationDetailsProps<T>>
) {
    return {
        supportsDetailsFor,
        showDetailsFor(notification: T, center: notificationCenter) {
            const close = () => {
                if (center.reactModalView() === view) {
                    center.reactModalView(null);
                }
            };

            const actions: NotificationDetailsActions = {
                close,
                dismiss: () => {
                    center.dismiss(notification);
                    close();
                },
                postpone: async (timeInSeconds) => {
                    await center.postpone(notification, timeInSeconds);
                    close();
                },
            };

            const view = ko.pureComputed(() => ({
                component,
                props: { key: notification.id, notification, actions },
            }));

            center.reactModalView(view);
        },
    };
}

export function isAlert<TReason extends AlertReason>(...reasons: TReason[]) {
    return (notification: abstractNotification): notification is alert<TReason> =>
        notification instanceof alert && (reasons.length === 0 || reasons.includes(notification.alertReason()));
}

export function isPerformanceHint<TReason extends PerformanceHintReason>(reason: TReason) {
    return (notification: abstractNotification): notification is performanceHint<TReason> =>
        notification instanceof performanceHint && notification.performanceHintReason() === reason;
}

export function isOperation(taskType: OperationType) {
    return (notification: abstractNotification): notification is operation =>
        notification instanceof operation && notification.taskType() === taskType;
}

export function isInstanceOf<T extends abstractNotification>(type: abstract new (...args: any[]) => T) {
    return (notification: abstractNotification): notification is T => notification instanceof type;
}
