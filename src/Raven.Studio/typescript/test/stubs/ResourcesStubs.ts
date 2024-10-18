import moment from "moment";

export class ResourcesStubs {
    static validValidateName(): Raven.Client.Util.NameValidation {
        return {
            IsValid: true,
            ErrorMessage: null,
        };
    }

    static invalidValidateName(): Raven.Client.Util.NameValidation {
        return {
            IsValid: false,
            ErrorMessage: "Invalid name",
        };
    }

    static databaseLocation(): Raven.Server.Web.Studio.DataDirectoryResult {
        return {
            List: [
                {
                    NodeTag: "A",
                    FullPath: `/`,
                    FreeSpaceInBytes: null,
                    FreeSpaceHumane: null,
                    TotalSpaceInBytes: null,
                    TotalSpaceHumane: null,
                    Error: "Cannot write to directory path: /",
                },
                {
                    NodeTag: "B",
                    FullPath: `C:/Workspace/ravendb/test/RachisTests/bin/Debug/net9.0/Databases/GetNewServer-B.0-3`,
                    FreeSpaceInBytes: 6126075904,
                    FreeSpaceHumane: "5.705 GBytes",
                    TotalSpaceInBytes: 20738408448,
                    TotalSpaceHumane: "19.314 GBytes",
                    Error: null,
                },
            ],
        };
    }

    static folderPathOptions_ServerLocal(): Raven.Server.Web.Studio.FolderPathOptions {
        return {
            List: ["/backup_test", "/other"],
        };
    }

    static restorePoints(): Raven.Server.Documents.PeriodicBackup.Restore.RestorePoints {
        return {
            List: [
                {
                    DateTime: "2024-01-02T13:36:23.1015138",
                    Location: "C:\\backup_test\\2024-01-02-13-36-02-7490729.ravendb-db1-C-backup",
                    FileName:
                        "C:\\backup_test\\2024-01-02-13-36-02-7490729.ravendb-db1-C-backup\\2024-01-02-13-36-02-7490729.ravendb-full-backup",
                    IsSnapshotRestore: false,
                    IsEncrypted: false,
                    IsIncremental: false,
                    FilesToRestore: 1,
                    DatabaseName: "db1",
                    NodeTag: "C",
                },
                {
                    DateTime: "2024-01-02T13:24:21.1651104",
                    Location: "C:\\backup_test\\2024-01-02-13-24-21-1477372.ravendb-d2-C-backup",
                    FileName:
                        "C:\\backup_test\\2024-01-02-13-24-21-1477372.ravendb-d2-C-backup\\2024-01-02-13-24-21-1477372.ravendb-full-backup",
                    IsSnapshotRestore: true,
                    IsEncrypted: true,
                    IsIncremental: false,
                    FilesToRestore: 1,
                    DatabaseName: "d2",
                    NodeTag: "C",
                },
                {
                    DateTime: "2024-01-02T13:21:06.7835361",
                    Location: "C:\\backup_test\\2024-01-02-13-21-06-2303869.ravendb-db1-A-backup",
                    FileName:
                        "C:\\backup_test\\2024-01-02-13-21-06-2303869.ravendb-db1-A-backup\\2024-01-02-13-21-06-2303869.ravendb-full-backup",
                    IsSnapshotRestore: false,
                    IsEncrypted: false,
                    IsIncremental: true,
                    FilesToRestore: 1,
                    DatabaseName: "db1",
                    NodeTag: "A",
                },
            ],
        };
    }

    static cloudBackupCredentials(): federatedCredentials {
        return {
            AwsSessionToken: "some-token",
            AwsAccessKey: "some-access-key",
            AwsSecretKey: "some-secret-key",
            AwsRegionName: "us-east-1",
            BucketName: "ravendb-some-us-east-1",
            RemoteFolderName: "some/free/db_N",
            BackupStorageType: "S3",
            Expires: moment().add(2, "hours").toString(),
            CustomServerUrl: null,
            ForcePathStyle: false,
            Disabled: false,
            GetBackupConfigurationScript: null,
        };
    }
}
