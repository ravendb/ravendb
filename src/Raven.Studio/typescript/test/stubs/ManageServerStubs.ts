import { AdminLogsMessage } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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
                        IsDictionary: false,
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

    static adminLogsConfiguration(): Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult {
        return {
            Logs: {
                Path: "C:\\Workspace\\ravendb",
                CurrentMinLevel: "Info",
                MinLevel: "Info",
                ArchiveAboveSizeInMb: 128,
                MaxArchiveDays: 3,
                MaxArchiveFiles: null,
                EnableArchiveFileCompression: false,
                CurrentFilters: [],
                CurrentLogFilterDefaultAction: "Neutral",
            },
            AuditLogs: {
                Path: null,
                Level: "Info",
                ArchiveAboveSizeInMb: 128,
                MaxArchiveDays: 3,
                MaxArchiveFiles: null,
                EnableArchiveFileCompression: false,
            },
            MicrosoftLogs: {
                CurrentMinLevel: "Error",
                MinLevel: "Error",
            },
            AdminLogs: {
                CurrentMinLevel: "Debug",
                CurrentFilters: [],
                CurrentLogFilterDefaultAction: "Neutral",
            },
        };
    }

    static eventListenerConfiguration(): Omit<
        Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration,
        "Persist"
    > {
        return {
            EventListenerMode: "ToLogFile",
            EventTypes: null,
            MinimumDurationInMs: 0,
            AllocationsLoggingIntervalInMs: 5000,
            AllocationsLoggingCount: 5,
        };
    }

    static trafficWatchConfiguration(): Omit<
        Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters,
        "Persist"
    > {
        return {
            TrafficWatchMode: "ToLogFile",
            Databases: [],
            StatusCodes: [],
            MinimumResponseSizeInBytes: 0,
            MinimumRequestSizeInBytes: 0,
            MinimumDurationInMs: 0,
            HttpMethods: [],
            ChangeTypes: [],
            CertificateThumbprints: [],
        };
    }

    static adminLogsMessages(): AdminLogsMessage[] {
        return [
            {
                Date: "2024-11-05 10:48:58.9905",
                Level: "INFO",
                ThreadID: "3",
                Resource: "LONG MESSAGE",
                Logger: "Raven.Server.RavenServerStartup",
                Message:
                    'PUT /databases/sample/admin/indexes - 500 - 22 ms|IndexCompilationException: Failed to compile index Orders/ByCompany\r\n\r\nusing System;\r\nusing System.Collections;\r\nusing System.Collections.Generic;\r\nusing System.Globalization;\r\nusing System.Linq;\r\nusing System.Text;\r\nusing System.Text.RegularExpressions;\r\nusing Lucene.Net.Documents;\r\nusing Raven.Client.Documents.Indexes;\r\nusing Raven.Server.Documents.Indexes.Static;\r\nusing Raven.Server.Documents.Indexes.Static.Linq;\r\nusing Raven.Server.Documents.Indexes.Static.Extensions;\r\n\r\nnamespace Raven.Server.Documents.Indexes.Static.Generated\r\n{\r\n    public class Index_Orders_ByCompany : StaticIndexBase\r\n    {\r\n        IEnumerable Map_0(IEnumerable<dynamic> docs)\r\n        {\r\n            foreach (var order in docs)\r\n            {\r\n                yield return new\r\n                {\r\n                    order.Company,\r\n                    Count = 1,\r\n                    Total = order.Lines.Sum((Func<dynamic, decimal>)(l => (decimal)((sda.Quantity * l.PricePerUnit) * (1 - l.Discount))))\r\n                };\r\n            }\r\n        }\r\n\r\n        public Index_Orders_ByCompany()\r\n        {\r\n            this.AddMap("Orders", this.Map_0);\r\n            this.Reduce = results =>\r\n                from result in results\r\n                group result by result.Company into g\r\n                select new\r\n                {\r\n                    Company = g.Key,\r\n                    Count = g.Sum((Func<dynamic, decimal>)(x => (decimal)(x.Count))),\r\n                    Total = g.Sum((Func<dynamic, decimal>)(x => (decimal)(x.Total)))\r\n                };\r\n            this.GroupByFields = new Raven.Server.Documents.Indexes.CompiledIndexField[]\r\n            {\r\n                new Raven.Server.Documents.Indexes.SimpleField("Company")\r\n            };\r\n            this.OutputFields = new System.String[]\r\n            {\r\n                "Company",\r\n                "Count",\r\n                "Total"\r\n            };\r\n            this.StackSizeInSelectClause = 0;\r\n        }\r\n    }\r\n}\r\n\r\n(26,86): error CS0103: The name \'sda\' does not exist in the current context\r\n, IndexDefinitionProperty=\'\', ProblematicText=\'\'   at Raven.Server.Documents.Indexes.Static.IndexCompiler.CompileInternal(String originalName, String cSharpSafeName, MemberDeclarationSyntax class, IndexDefinition definition) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompiler.cs:line 247\r\n   at Raven.Server.Documents.Indexes.Static.IndexCompiler.Compile(IndexDefinition definition, Int64 indexVersion) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompiler.cs:line 175\r\n   at Raven.Server.Documents.Indexes.Static.IndexCompilationCache.GenerateIndex(IndexDefinition definition, RavenConfiguration configuration, IndexType type, Int64 indexVersion) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompilationCache.cs:line 134\r\n   at Raven.Server.Documents.Indexes.Static.IndexCompilationCache.<>c__DisplayClass2_0.<GetDocumentsIndexInstance>b__1() in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompilationCache.cs:line 47\r\n   at System.Lazy`1.ViaFactory(LazyThreadSafetyMode mode)\r\n   at System.Lazy`1.ExecutionAndPublication(LazyHelper executionAndPublication, Boolean useDefaultConstructor)\r\n   at System.Lazy`1.CreateValue()\r\n   at Raven.Server.Documents.Indexes.Static.IndexCompilationCache.GetDocumentsIndexInstance(IndexDefinition definition, RavenConfiguration configuration, IndexType type, Int64 indexVersion) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompilationCache.cs:line 51\r\n   at Raven.Server.Documents.Indexes.Static.IndexCompilationCache.GetIndexInstance(IndexDefinition definition, RavenConfiguration configuration, Int64 indexVersion) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\Static\\IndexCompilationCache.cs:line 30\r\n   at Raven.Server.Documents.Indexes.AbstractIndexCreateController.ValidateStaticIndexAsync(IndexDefinition definition) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\AbstractIndexCreateController.cs:line 76\r\n   at Raven.Server.Documents.Indexes.AbstractIndexCreateController.CreateIndexAsync(IndexDefinition definition, String raftRequestId, String source) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Indexes\\AbstractIndexCreateController.cs:line 115\r\n   at Raven.Server.Documents.Handlers.Admin.Processors.Indexes.AbstractAdminIndexHandlerProcessorForPut`2.ExecuteAsync() in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Handlers\\Admin\\Processors\\Indexes\\AbstractAdminIndexHandlerProcessorForPut.cs:line 78\r\n   at Raven.Server.Documents.Handlers.Admin.AdminIndexHandler.Put() in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Documents\\Handlers\\Admin\\AdminIndexHandler.cs:line 25\r\n   at Raven.Server.Routing.RequestRouter.HandlePath(RequestHandlerContext reqCtx) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\Routing\\RequestRouter.cs:line 425\r\n   at Raven.Server.RavenServerStartup.RequestHandler(HttpContext context) in C:\\Workspace\\ravendb_7.0\\src\\Raven.Server\\RavenServerStartup.cs:line 191',
                _meta: {
                    id: "1",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:48:59.0080",
                Level: "DEBUG",
                ThreadID: "24",
                Resource: "Server",
                Logger: "Raven.Server.RavenServerStartup",
                Message: "GET /admin/logs/configuration - 200 - 0 ms",
                _meta: {
                    id: "2",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:48:59.0575",
                Level: "WARN",
                ThreadID: "24",
                Resource: "Server",
                Logger: "Raven.Server.RavenServerStartup",
                Message: "GET /admin/event-listener/configuration - 200 - 0 ms",
                _meta: {
                    id: "4",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:48:59.4414",
                Level: "ERROR",
                ThreadID: "11",
                Resource: "Sparrow",
                Logger: "Sparrow.LowMemory.LowMemoryNotification",
                Message:
                    "Running 12 low memory handlers with severity: None. Commit charge: 27.315 GBytes / 36.102 GBytes, Memory: 20.171 GBytes / 31.352 GBytes, Available memory for processing: 11.181 GBytes, Dirty memory: 0 Bytes, Managed memory: 521.44 MBytes, Unmanaged allocations: 6.41 MBytes, Lucene managed: 0 Bytes, Lucene unmanaged: 0 Bytes",
                _meta: {
                    id: "5",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:49:04.4572",
                Level: "FATAL",
                ThreadID: "11",
                Resource: "Sparrow",
                Logger: "Sparrow.LowMemory.LowMemoryNotification",
                Message:
                    "Running 12 low memory handlers with severity: None. Commit charge: 27.424 GBytes / 36.102 GBytes, Memory: 20.237 GBytes / 31.352 GBytes, Available memory for processing: 11.114 GBytes, Dirty memory: 0 Bytes, Managed memory: 522.9 MBytes, Unmanaged allocations: 6.46 MBytes, Lucene managed: 0 Bytes, Lucene unmanaged: 0 Bytes",
                _meta: {
                    id: "6",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:49:09.4670",
                Level: "OFF",
                ThreadID: "11",
                Resource: "Sparrow",
                Logger: "Sparrow.LowMemory.LowMemoryNotification",
                Message:
                    "Running 12 low memory handlers with severity: None. Commit charge: 27.298 GBytes / 36.102 GBytes, Memory: 20.224 GBytes / 31.352 GBytes, Available memory for processing: 11.128 GBytes, Dirty memory: 0 Bytes, Managed memory: 524.31 MBytes, Unmanaged allocations: 6.63 MBytes, Lucene managed: 0 Bytes, Lucene unmanaged: 0 Bytes",
                _meta: {
                    id: "7",
                    isExpanded: false,
                },
            },
            {
                Date: "2024-11-05 10:49:11.4588",
                Level: "TRACE",
                ThreadID: "24",
                Resource: "Server",
                Logger: "Raven.Server.RavenServerStartup",
                Message: "GET /studio/index.html - 304 - 0 ms",
                _meta: {
                    id: "8",
                    isExpanded: false,
                },
            },
        ];
    }
}
