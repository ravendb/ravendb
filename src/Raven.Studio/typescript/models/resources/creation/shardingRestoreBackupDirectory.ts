export default class shardingRestoreBackupDirectory {
    nodeTag: KnockoutObservable<string>;
    directoryPath: KnockoutObservable<string>;
    directoryPathOptions: KnockoutObservableArray<string>

    validationGroup: KnockoutValidationGroup;  

    constructor() {
        this.directoryPath = ko.observable("");
        this.nodeTag = ko.observable("");
        this.directoryPathOptions = ko.observableArray();

        this.initValidation();
    }

    initValidation() {
        this.nodeTag.extend({
            required: true
        });

        this.directoryPath.extend({
            required: true
        });
       
        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            directoryPath: this.directoryPath,
        });
    }
}
