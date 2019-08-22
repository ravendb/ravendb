/// <reference path="../../../../typings/tsd.d.ts"/>

class googleCloudCredentials {
    bucketName = ko.observable<string>();
    googleCredentialsJson = ko.observable<string>();
    remoteFolderName = ko.observable<string>();

    toDto()  {
        const dto: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings = {
            BucketName : this.bucketName(),
            Disabled : false,
            GetBackupConfigurationScript : null,
            GoogleCredentialsJson : this.googleCredentialsJson(),
            RemoteFolderName : this.remoteFolderName()
        };

        return dto;
    }

    static empty() {
        const dto: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings = {
            BucketName : null,
            Disabled : false,
            GetBackupConfigurationScript : null,
            GoogleCredentialsJson : null,
            RemoteFolderName : null
        };

        return dto;
    }
}

export = googleCloudCredentials;
