/// <reference path="../../../../typings/tsd.d.ts" />
import virtualNotification = require("common/notifications/models/virtualNotification");
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class virtualBulkInsert extends virtualNotification {
    
    static readonly Id = "virtual$$bulkInsert";

    operations = ko.observableArray<virtualBulkOperationItem>([]);
    
    constructor(db: database) {
        super(db, {
            Id: virtualBulkInsert.Id,
            IsPersistent: false,
            Type: "CumulativeBulkInsert",
            Database: db.name,
            
            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });
        
        this.title("Bulk inserts");
        this.severity("Success");
    }
    
    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);
        
        const progress = dto.State.Progress as Raven.Client.Documents.Operations.BulkInsertProgress;
        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.BulkOperationResult;
        const bulkInsertInfo = bulkResult || progress;
        
        const item = {
            id: dto.Id,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            totalItemsProcessed: bulkInsertInfo.Total,
            documentsProcessed: bulkInsertInfo.DocumentsProcessed,
            attachmentsProcessed: bulkInsertInfo.AttachmentsProcessed,
            countersProcessed: bulkInsertInfo.CountersProcessed,
            timeSeriesProcessed: bulkInsertInfo.TimeSeriesProcessed,
            
        } as virtualBulkOperationItem;
        
        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);
        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }
        
        const totalItemsCount = _.sumBy(this.operations(), x => x.totalItemsProcessed);
        this.message(pluralizeHelpers.pluralize(this.operations().length, "bulk insert", "bulk inserts")
            + " to database " + this.database.name
            + " completed successfully. "
            + pluralizeHelpers.pluralize(totalItemsCount, " item was inserted.", "items were inserted.") );
    }
}

export = virtualBulkInsert;
