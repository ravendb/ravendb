import virtualNotification = require("common/notifications/models/virtualNotification");


abstract class groupedVirtualNotification<T extends { id: string; operationId?: number }> extends virtualNotification {
    operations = ko.observableArray<T>([]);
}

export = groupedVirtualNotification;
