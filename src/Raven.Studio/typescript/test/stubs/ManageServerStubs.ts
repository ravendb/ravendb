import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import ConfigurationEntryServerValue = Raven.Server.Config.ConfigurationEntryServerValue;

export class ManageServerStubs {
    static getSampleClientGlobalConfiguration(): ClientConfiguration {
        return {
            Disabled: false,
            Etag: 103,
            IdentityPartsSeparator: ".",
            MaxNumberOfRequestsPerSession: 32,
        };
    }

    static getSampleClientDatabaseConfiguration(): ClientConfiguration {
        return {
            Disabled: false,
            Etag: 132,
            IdentityPartsSeparator: ";",
            LoadBalanceBehavior: "UseSessionContext",
            ReadBalanceBehavior: "RoundRobin",
        };
    }

    static serverWideCustomAnalyzers(): AnalyzerDefinition[] {
        return [
            { Code: "server-analyzer-code-1", Name: "First Server analyzer" },
            { Code: "server-analyzer-code-2", Name: "Second Server analyzer" },
            { Code: "server-analyzer-code-3", Name: "Third Server analyzer" },
            { Code: "server-analyzer-code-4", Name: "Fourth Server analyzer" },
        ];
    }

    static serverWideCustomSorters(): SorterDefinition[] {
        return [
            { Code: "server-sorter-code-1", Name: "First Server sorter" },
            { Code: "server-sorter-code-2", Name: "Second Server sorter" },
            { Code: "server-sorter-code-3", Name: "Third Server sorter" },
            { Code: "server-sorter-code-4", Name: "Fourth Server sorter" },
        ];
    }

    static serverSettings(): { Settings: ConfigurationEntryServerValue[] } {
        return {
            Settings: [
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["Setup.Mode"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "None",
                        IsDefaultValueDynamic: false,
                        Description: "Determines what kind of security was chosen during setup.",
                        Type: "Enum",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: ["None", "Initial", "LetsEncrypt", "Secured", "Unsecured"],
                    },
                    ServerValues: {
                        "Setup.Mode": {
                            Value: "None",
                            HasValue: true,
                            HasAccess: true,
                            PendingValue: null,
                        },
                    },
                },
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["Setup.Certificate.Path"],
                        Scope: "ServerWideOnly",
                        DefaultValue: null,
                        IsDefaultValueDynamic: false,
                        Description: "Determines where to save the initial server certificate during setup.",
                        Type: "String",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["AcmeUrl"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "https://acme-v02.api.letsencrypt.org/directory",
                        IsDefaultValueDynamic: false,
                        Description:
                            "The URLs which the server should contact when requesting certificates with the ACME protocol.",
                        Type: "String",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["ThrowIfAnyIndexCannotBeOpened"],
                        Scope: "ServerWideOrPerDatabase",
                        DefaultValue: "False",
                        IsDefaultValueDynamic: false,
                        Description: "Indicates if we should throw an exception if any index could not be opened",
                        Type: "Boolean",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["Features.Availability"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "Stable",
                        IsDefaultValueDynamic: false,
                        Description: "Indicates what set of features should be available",
                        Type: "Enum",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: ["Stable", "Experimental"],
                    },
                    ServerValues: {
                        "Features.Availability": {
                            Value: "Experimental",
                            HasValue: true,
                            HasAccess: true,
                            PendingValue: null,
                        },
                    },
                },
                {
                    Metadata: {
                        Category: "Core",
                        Keys: ["Testing.EchoSocket.Port"],
                        Scope: "ServerWideOnly",
                        DefaultValue: null,
                        IsDefaultValueDynamic: false,
                        Description:
                            "EXPERT: Allow to test network status of the system to discover kernel level issues",
                        Type: "Integer",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: true,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Security",
                        Keys: ["Security.DisableHttpsRedirection"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "False",
                        IsDefaultValueDynamic: false,
                        Description:
                            "Disable automatic redirection when listening to HTTPS. By default, when using port 443, RavenDB redirects all incoming HTTP traffic on port 80 to HTTPS on port 443.",
                        Type: "Boolean",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Security",
                        Keys: ["Security.DisableHsts"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "False",
                        IsDefaultValueDynamic: false,
                        Description: "Disable HTTP Strict Transport Security.",
                        Type: "Boolean",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Security",
                        Keys: ["Security.AuditLog.FolderPath"],
                        Scope: "ServerWideOnly",
                        DefaultValue: null,
                        IsDefaultValueDynamic: false,
                        Description:
                            "The folder path where RavenDB stores audit log files. Setting the path enables writing to the audit log.",
                        Type: "Path",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: null,
                    },
                    ServerValues: {},
                },
                {
                    Metadata: {
                        Category: "Security",
                        Keys: ["Security.UnsecuredAccessAllowed"],
                        Scope: "ServerWideOnly",
                        DefaultValue: "Local",
                        IsDefaultValueDynamic: false,
                        Description:
                            "If authentication is disabled, set address range type for which server access is unsecured (None | Local | PrivateNetwork | PublicNetwork).",
                        Type: "Enum",
                        SizeUnit: null,
                        TimeUnit: null,
                        MinValue: null,
                        IsArray: false,
                        IsNullable: false,
                        IsSecured: false,
                        AvailableValues: ["None", "Local", "PrivateNetwork", "PublicNetwork"],
                    },
                    ServerValues: {
                        "Security.UnsecuredAccessAllowed": {
                            Value: "PublicNetwork",
                            HasValue: true,
                            HasAccess: true,
                            PendingValue: null,
                        },
                    },
                },
            ],
        };
    }
}
