/// <reference path="../../../../typings/tsd.d.ts" />
import virtualNotification = require("common/notifications/models/virtualNotification");
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class virtualUpdateByQuery extends virtualNotification {

    static readonly Id = "virtual$$updateByQuery";

    operations = ko.observableArray<queryBasedVirtualBulkOperationItem>([]);

    constructor(db: database) {
        super(db, {
            Id: virtualUpdateByQuery.Id,
            IsPersistent: false,
            Type: "CumulativeUpdateByQuery",
            Database: db.name,

            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });

        this.title("Update by query");
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
            totalItemsProcessed: bulkResult.Total,
            indexOrCollectionUsed: dto.Message,
            query: (dto.DetailedDescription as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query
        } as queryBasedVirtualBulkOperationItem;

        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }

        this.message(pluralizeHelpers.pluralize(this.operations().length, "operation", "operations")
            + " has been completed successfully. ");
    }
}

export = virtualUpdateByQuery;
