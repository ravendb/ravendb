import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");

class googleCloudSettings extends backupSettings {
    bucket = ko.observable<string>();
    remoteFolderName = ko.observable<string>();
    googleCredentialsJson = ko.observable<string>();

    targetOperation: string;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings, targetOperation: string) {
        super(dto, "GoogleCloud");

        this.bucket(dto.BucketName);
        this.remoteFolderName(dto.RemoteFolderName || "");
        this.googleCredentialsJson(dto.GoogleCredentialsJson);

        this.targetOperation = targetOperation;
        
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.bucket,
            this.remoteFolderName,
            this.googleCredentialsJson,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    compositionComplete(view: Element, container: HTMLElement) {
        popoverUtils.longWithHover($(".bucket-gcs-info", container),
            {
                content: tasksCommonContent.textForPopoverGCS("Bucket", this.targetOperation)
            });
    }
    
    initValidation() {
        const allowedCharactersRegExp = /^[a-z0-9._-]+$/;
        const allowedBeginningCharactersRegExp = /^[a-z0-9]+$/;
        const firstDashRuleRegExp = /\.-/;
        const secondDashRuleRegExp = /\.\./;
        const thirdDashRuleRegExp = /-\./;
        const fourthDashesRegExp = /_\./;
        const fifthDashesRegExp = /\._/;
        this.bucket.extend({
            validation: [
                {
                    validator: (bucket: string) => bucket && bucket.length >= 3 && bucket.length <= 222,
                    message: "Bucket name must contain 3 to 63 characters." +
                        "Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters"
                },
                {
                    validator: (bucket: string) => allowedCharactersRegExp.test(bucket),
                    message: "Bucket name must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.)"
                },
                {
                    validator: (bucket: string) => bucket && allowedBeginningCharactersRegExp.test(bucket[0]) && allowedBeginningCharactersRegExp.test(bucket[bucket.length - 1]),
                    message: "Bucket name must start and end with a number or letter"
                },
                {
                    validator: (bucket: string) => !firstDashRuleRegExp.test(bucket) && !secondDashRuleRegExp.test(bucket) &&
                        !thirdDashRuleRegExp.test(bucket) && !fourthDashesRegExp.test(bucket) &&
                        !fifthDashesRegExp.test(bucket),
                    message: "Dashes, periods and underscores are not permitted to be adjacent to another"
                }
            ]
        });

        this.googleCredentialsJson.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: (json: string) => json && json.includes("\"type\""),
                    message: "Google credentials json is missing 'type' field"
                },
                {
                    validator: (json: string) => json && json.includes("\"private_key\""),
                    message: "Google credentials json is missing 'private_key' field"
                },
                {
                    validator: (json: string) => json && json.includes("\"client_email\""),
                    message: "Google credentials json is missing 'client_email' field"
                }
            ]
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            bucket: this.bucket,
            accountKey: this.googleCredentialsJson
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.GoogleCloudSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
        dto.BucketName = this.bucket();
        dto.RemoteFolderName = this.remoteFolderName();
        dto.GoogleCredentialsJson = this.googleCredentialsJson();

        return genUtils.trimProperties(dto, ["RemoteFolderName"]);
    }

    static empty(targetOperation: string): googleCloudSettings {
        return new googleCloudSettings({
            Disabled: true,
            RemoteFolderName: null,
            GoogleCredentialsJson: null,
            BucketName: null,
            GetBackupConfigurationScript: null
        }, targetOperation);
    }
}

export = googleCloudSettings;
