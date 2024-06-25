/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import moment = require("moment");
import groupedVirtualNotification from "common/notifications/models/groupedVirtualNotification";

class virtualUpdateByQueryFailures extends groupedVirtualNotification<queryBasedVirtualBulkOperationFailureItem> {

    static readonly Id = "virtual$$updateByQueryFailures";

    constructor(db: database) {
        super(db, {
            Id: virtualUpdateByQueryFailures.Id,
            IsPersistent: false,
            Type: "CumulativeUpdateByQueryFailures",
            Database: db.name,

            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });

        this.title("Update by query");
        this.severity("Error");
    }

    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);

        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);

        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;

        const item = {
            id: dto.Id,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            error: bulkResult.Error,
            errorMsg: bulkResult.Message,
            query: (dto.DetailedDescription as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query
        } as queryBasedVirtualBulkOperationFailureItem;

        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }

        this.message(pluralizeHelpers.pluralize(this.operations().length, "operation", "operations")
            + " has been completed with an error. ");
    }
}

export = virtualUpdateByQueryFailures;
