/// <reference path="../../../../typings/tsd.d.ts" />
import virtualNotification = require("common/notifications/models/virtualNotification");
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class virtualDeleteByQueryFailures extends virtualNotification {
    
    static readonly Id = "virtual$$deleteByQueryFailures";
    
    operations = ko.observableArray<queryBasedVirtualBulkOperationFailureItem>([]);
    
    constructor(db: database) {
        super(db, {
            Id: virtualDeleteByQueryFailures.Id,
            IsPersistent: false,
            Type: "CumulativeDeleteByQueryFailures",
            Database: db.name,
            
            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });
        
        this.title("Delete by query");
        this.severity("Error");
    }
    
    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);
        
        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);
        
        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;
        
        const query = dto.TaskType === "DeleteByQuery" 
            ? (dto.DetailedDescription as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query
            : "n/a";
        
        const item = {
            id: dto.Id,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            query: query,
            error: bulkResult.Error,
            errorMsg: bulkResult.Message
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

export = virtualDeleteByQueryFailures;
