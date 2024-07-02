import virtualNotification from "common/notifications/models/virtualNotification";


abstract class groupedVirtualNotification<T extends { id: string }> extends virtualNotification {
    operations = ko.observableArray<T>([]);
}

export = groupedVirtualNotification;
