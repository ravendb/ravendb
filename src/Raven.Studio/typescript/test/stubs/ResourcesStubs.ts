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
                    FullPath: `C:/Workspace/ravendb/test/RachisTests/bin/Debug/net8.0/Databases/GetNewServer-B.0-3`,
                    FreeSpaceInBytes: 6126075904,
                    FreeSpaceHumane: "5.705 GBytes",
                    TotalSpaceInBytes: 20738408448,
                    TotalSpaceHumane: "19.314 GBytes",
                    Error: null,
                },
            ],
        };
    }

    static localFolderPathOptions(): Raven.Server.Web.Studio.FolderPathOptions {
        return {
            List: ["/bin", "/boot", "/data", "/dev", "/etc"],
        };
    }
}
