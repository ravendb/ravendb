
export default class shardingRestoreBackupDirectory {
    nodeTag: KnockoutObservable<string>;
    directoryPath: KnockoutObservable<string>;
    directoryPathOptions: KnockoutObservableArray<string>

    constructor() {
        this.directoryPath = ko.observable("");
        this.nodeTag = ko.observable("");
        this.directoryPathOptions = ko.observableArray();
    }
}