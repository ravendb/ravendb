/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import moment = require("moment");
import groupedVirtualNotification from "common/notifications/models/groupedVirtualNotification";

class virtualBulkInsertFailures extends groupedVirtualNotification<virtualBulkOperationFailureItem> {
    
    static readonly Id = "virtual$$bulkInsertFailures";

    constructor(db: database) {
        super(db, {
            Id: virtualBulkInsertFailures.Id,
            IsPersistent: false,
            Type: "CumulativeBulkInsertFailures",
            Database: db.name,
            
            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });
        
        this.title("Bulk inserts");
        this.severity("Error");
    }
    
    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);
        
        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;

        const item: virtualBulkOperationFailureItem = {
            id: dto.Id,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            errorMsg: bulkResult.Message,
            error: bulkResult.Error
        };
        
        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);
        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }
        
        this.message(pluralizeHelpers.pluralize(this.operations().length, "bulk insert", "bulk inserts")
            + " to database " + this.database.name
            + " has been completed with an error. ");
    }
}

export = virtualBulkInsertFailures;
