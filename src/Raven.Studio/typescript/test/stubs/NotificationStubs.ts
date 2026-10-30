import moment from "moment";
import abstractNotification from "common/notifications/models/abstractNotification";
import alert from "common/notifications/models/alert";
import performanceHint from "common/notifications/models/performanceHint";
import recentError from "common/notifications/models/recentError";
import operation from "common/notifications/models/operation";
import groupedVirtualNotification from "common/notifications/models/groupedVirtualNotification";
import database from "models/resources/database";
import { AlertDetails, PerformanceHintDetails } from "common/notifications/models/notificationDetailsByReason";

type AlertRaised = Raven.Server.NotificationCenter.Notifications.AlertRaised;
type PerformanceHintDto = Raven.Server.NotificationCenter.Notifications.PerformanceHint;
type AlertReason = Raven.Server.NotificationCenter.Notifications.AlertReason;
type PerformanceHintReason = Raven.Server.NotificationCenter.Notifications.PerformanceHintReason;
type OperationChanged = Raven.Server.NotificationCenter.Notifications.OperationChanged;
type OperationState = Raven.Client.Documents.Operations.OperationState;
type OperationType = Raven.Server.Documents.Operations.OperationType;

const createdAt = "2026-10-20T10:00:00.0000000Z";

export class NotificationStubs {
    static performanceHint<TReason extends PerformanceHintReason>(
        reason: TReason,
        details: PerformanceHintDetails<TReason>,
        overrides: Partial<PerformanceHintDto> = {}
    ): performanceHint<TReason> {
        const hint = new performanceHint<TReason>(null, {
            Id: "PerformanceHint/" + reason,
            Type: "PerformanceHint",
            Title: "Performance hint title",
            Message: "Performance hint message",
            Severity: "Warning",
            CreatedAt: createdAt,
            IsPersistent: true,
            Database: "db1",
            Source: "",
            Reason: reason,
            Details: details,
            ...overrides,
        });

        return NotificationStubs.withDatabaseWriteAccess(hint, overrides.Database ?? "db1");
    }

    static alert<TReason extends AlertReason>(
        reason: TReason,
        details: AlertDetails<TReason>,
        overrides: Partial<AlertRaised> = {}
    ): alert<TReason> {
        const alertNotification = new alert<TReason>(null, {
            Id: "AlertRaised/" + reason,
            Type: "AlertRaised",
            Title: "Alert title",
            Message: "Alert message",
            Severity: "Warning",
            CreatedAt: createdAt,
            IsPersistent: true,
            Database: "db1",
            Key: null,
            Reason: reason,
            Details: details,
            ...overrides,
        });

        return NotificationStubs.withDatabaseWriteAccess(alertNotification, overrides.Database ?? "db1");
    }

    static recentError(overrides: Partial<recentErrorDto> = {}): recentError {
        const error = new recentError({
            Id: "RecentError/1",
            Type: "RecentError",
            Title: "Recent error title",
            Message: "Recent error message",
            Severity: "Error",
            CreatedAt: createdAt,
            IsPersistent: false,
            Database: null,
            Details: null,
            ...overrides,
        });

        error.readOnly = false;
        return error;
    }

    static operation(
        taskType: OperationType,
        state: OperationState,
        overrides: Partial<OperationChanged> = {}
    ): operation {
        const operationNotification = new operation(overrides.Database ?? "db1", {
            Id: "Operation/" + taskType,
            Type: "OperationChanged",
            Title: "Operation title",
            Message: "Operation message",
            Severity: state.Status === "Faulted" ? "Error" : "Success",
            CreatedAt: createdAt,
            IsPersistent: false,
            Database: "db1",
            DetailedDescription: null,
            StartTime: createdAt,
            EndTime: state.Status === "InProgress" ? null : "2026-10-20T10:01:16.0000000Z",
            Killable: state.Status === "InProgress",
            OperationId: 1,
            State: state,
            TaskType: taskType,
            ...overrides,
        });

        operationNotification.readOnly = false;
        return operationNotification;
    }

    static virtualOperation<T extends groupedVirtualNotification<{ id: string }>>(
        type: new (db: database) => T,
        operations: ReturnType<T["operations"]>
    ): T {
        const notification = new type({ name: "db1" } as database);
        notification.createdAt(moment.utc(createdAt));
        notification.operations(operations);
        return notification;
    }

    private static withDatabaseWriteAccess<T extends abstractNotification>(notification: T, databaseName: string): T {
        notification.databaseName = databaseName;
        notification.readOnly = false;
        return notification;
    }
}
