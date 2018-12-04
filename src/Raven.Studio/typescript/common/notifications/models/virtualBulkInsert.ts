/// <reference path="../../../../typings/tsd.d.ts" />
import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class virtualBulkInsert extends abstractNotification {
    
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
        
        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);
        
        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.BulkOperationResult;
        
        const item = {
            id: dto.Id,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            items: bulkResult.Total
        } as virtualBulkOperationItem;
        
        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }
        
        const totalDocumentsCount = _.sumBy(this.operations(), x => x.items);
        this.message(pluralizeHelpers.pluralize(this.operations().length, "bulk insert", "bulk inserts")
            + " completed successfully. "
            + pluralizeHelpers.pluralize(totalDocumentsCount, " document was created.", "documents were created.") );
    }
}

export = virtualBulkInsert;
