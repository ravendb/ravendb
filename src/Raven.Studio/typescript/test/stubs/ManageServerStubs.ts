import { AdminLogsMessage } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import moment from "moment";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import ConfigurationEntryServerValue = Raven.Server.Config.ConfigurationEntryServerValue;

export class ManageServerStubs {
    static getClusterLogEntry(): Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry {
        return {
            Term: 1,
            Index: 9,
            SizeInBytes: 125,
            CommandType: "TestCommandWithRaftId",
            Flags: "StateMachineCommand",
            CreateAt: "2025-03-04T19:41:05.7366020",
            Entry: {
                Type: "TestCommandWithRaftId",
                UniqueRequestId: "1fa89e68-231f-49fb-a19b-157bd7ed1b7e",
                Name: "test",
                Value: null,
            },
        };
    }
    private static getClusterLogBase(withError: boolean): Omit<Raven.Server.Rachis.RaftDebugView, "Role"> {
        return {
            Term: 1,
            CommandsVersion: {
                Cluster: 62001,
                Local: 62001,
            },
            Since: "2025-03-04T19:41:04.7172300Z",
            Log: {
                LastAppendedTime: null,
                LastCommitedTime: "2025-03-04T19:41:05.7139309Z",
                CommitIndex: 8,
                LastTruncatedIndex: 4,
                LastTruncatedTerm: 1,
                FirstEntryIndex: 5,
                LastLogEntryIndex: 37,
                TotalEntries: 35,
                CriticalError: withError
                    ? {
                          Id: "AlertRaised/UnrecoverableClusterError/9",
                          Title: "Unrecoverable Cluster Error at Index 9",
                          Message:
                              "Unrecoverable exception at command type 'TestCommandWithRaftId', execution will be retried later.",
                          CreatedAt: "2025-03-04T19:44:18.5210160Z",
                          Exception:
                              '{"$type":"Raven.Server.NotificationCenter.Notifications.Details.ExceptionDetails, Raven.Server","Exception":"Raven.Server.ServerWide.UnknownClusterCommandException: The command \'TestCommandWithRaftId\' is unknown and cannot be executed on server with version \'6.2.4-custom-62\'.\\r\\nUpdating this node version to match the rest should resolve this issue.\\r\\n   at Raven.Server.ServerWide.ClusterStateMachine.Apply(ClusterOperationContext context, BlittableJsonReaderObject cmd, Int64 index, Leader leader, ServerStore serverStore) in C:\\\\workspaces\\\\ravendb\\\\src\\\\Raven.Server\\\\ServerWide\\\\ClusterStateMachine.cs:line 685"}',
                      }
                    : null,
                Logs: [
                    {
                        Term: 1,
                        Index: 37,
                        SizeInBytes: 204,
                        CommandType: "UpdateServerPublishedUrlsCommand",
                        Flags: "StateMachineCommand",
                        CreateAt: "2025-03-04T19:44:09.9211893",
                        Entry: null,
                    },
                    {
                        Term: 1,
                        Index: 36,
                        SizeInBytes: 204,
                        CommandType: "UpdateServerPublishedUrlsCommand",
                        Flags: "StateMachineCommand",
                        CreateAt: "2025-03-04T19:43:52.0946001",
                        Entry: null,
                    },
                    {
                        Term: 1,
                        Index: 8,
                        SizeInBytes: 674,
                        CommandType: "UpdateLicenseLimitsCommand",
                        Flags: "StateMachineCommand",
                        CreateAt: "2025-03-04T19:41:05.7095347",
                        Entry: null,
                    },
                ],
            },
        };
    }
    static getClusterLogLeader(): Raven.Server.Rachis.LeaderDebugView {
        return {
            ...ManageServerStubs.getClusterLogBase(false),
            Role: "Leader",
            ElectionReason: "I'm the only one in the cluster, so I'm the leader",
            ConnectionToPeers: [
                {
                    Status: "Failed to create a connection to node C at http://127.0.0.1:60660.\r\nSystem.AggregateException: One or more errors occurred. (An exception occurred while contacting http://127.0.0.1:60660/info/tcp?tag=Cluster.\r\nSystem.Net.Http.HttpRequestException: No connection could be made because the target machine actively refused it. (127.0.0.1:60660)\r\n ---> System.Net.Sockets.SocketException (10061): No connection could be made because the target machine actively refused it.\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.ThrowException(SocketError error, CancellationToken cancellationToken)\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.System.Threading.Tasks.Sources.IValueTaskSource.GetResult(Int16 token)\r\n   at System.Net.Sockets.Socket.<ConnectAsync>g__WaitForConnectWithCancellation|285_0(AwaitableSocketAsyncEventArgs saea, ValueTask connectTask, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   --- End of inner exception stack trace ---\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.CreateHttp11ConnectionAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)\r\n   at System.Threading.Tasks.TaskCompletionSourceWithCancellation`1.WaitWithCancellationAsync(CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.SendWithVersionDetectionAndRetryAsync(HttpRequestMessage request, Boolean async, Boolean doRequestAuth, CancellationToken cancellationToken)\r\n   at System.Net.Http.RedirectHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.DecompressionHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpClient.<SendAsync>g__Core|83_0(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationTokenSource cts, Boolean disposeCts, CancellationTokenSource pendingRequestsCts, CancellationToken originalCancellationToken)\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 118\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 129\r\n   at Raven.Client.Http.RequestExecutor.SendAsync[TResult](ServerNode chosenNode, RavenCommand`1 command, SessionInfo sessionInfo, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1200\r\n   at Raven.Client.Http.RequestExecutor.SendRequestToServer[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, HttpRequestMessage request, String url, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1112.\r\nThe server at http://127.0.0.1:60660/info/tcp?tag=Cluster responded with status code: ServiceUnavailable.)\r\n ---> Raven.Client.Exceptions.RavenException: An exception occurred while contacting http://127.0.0.1:60660/info/tcp?tag=Cluster.\r\nSystem.Net.Http.HttpRequestException: No connection could be made because the target machine actively refused it. (127.0.0.1:60660)\r\n ---> System.Net.Sockets.SocketException (10061): No connection could be made because the target machine actively refused it.\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.ThrowException(SocketError error, CancellationToken cancellationToken)\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.System.Threading.Tasks.Sources.IValueTaskSource.GetResult(Int16 token)\r\n   at System.Net.Sockets.Socket.<ConnectAsync>g__WaitForConnectWithCancellation|285_0(AwaitableSocketAsyncEventArgs saea, ValueTask connectTask, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   --- End of inner exception stack trace ---\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.CreateHttp11ConnectionAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)\r\n   at System.Threading.Tasks.TaskCompletionSourceWithCancellation`1.WaitWithCancellationAsync(CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.SendWithVersionDetectionAndRetryAsync(HttpRequestMessage request, Boolean async, Boolean doRequestAuth, CancellationToken cancellationToken)\r\n   at System.Net.Http.RedirectHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.DecompressionHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpClient.<SendAsync>g__Core|83_0(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationTokenSource cts, Boolean disposeCts, CancellationTokenSource pendingRequestsCts, CancellationToken originalCancellationToken)\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 118\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 129\r\n   at Raven.Client.Http.RequestExecutor.SendAsync[TResult](ServerNode chosenNode, RavenCommand`1 command, SessionInfo sessionInfo, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1200\r\n   at Raven.Client.Http.RequestExecutor.SendRequestToServer[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, HttpRequestMessage request, String url, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1112.\r\nThe server at http://127.0.0.1:60660/info/tcp?tag=Cluster responded with status code: ServiceUnavailable.\r\n ---> System.Net.Http.HttpRequestException: No connection could be made because the target machine actively refused it. (127.0.0.1:60660)\r\n ---> System.Net.Sockets.SocketException (10061): No connection could be made because the target machine actively refused it.\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.ThrowException(SocketError error, CancellationToken cancellationToken)\r\n   at System.Net.Sockets.Socket.AwaitableSocketAsyncEventArgs.System.Threading.Tasks.Sources.IValueTaskSource.GetResult(Int16 token)\r\n   at System.Net.Sockets.Socket.<ConnectAsync>g__WaitForConnectWithCancellation|285_0(AwaitableSocketAsyncEventArgs saea, ValueTask connectTask, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   --- End of inner exception stack trace ---\r\n   at System.Net.Http.HttpConnectionPool.ConnectToTcpHostAsync(String host, Int32 port, HttpRequestMessage initialRequest, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.ConnectAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.CreateHttp11ConnectionAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)\r\n   at System.Threading.Tasks.TaskCompletionSourceWithCancellation`1.WaitWithCancellationAsync(CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpConnectionPool.SendWithVersionDetectionAndRetryAsync(HttpRequestMessage request, Boolean async, Boolean doRequestAuth, CancellationToken cancellationToken)\r\n   at System.Net.Http.RedirectHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.DecompressionHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n   at System.Net.Http.HttpClient.<SendAsync>g__Core|83_0(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationTokenSource cts, Boolean disposeCts, CancellationTokenSource pendingRequestsCts, CancellationToken originalCancellationToken)\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 118\r\n   at Raven.Client.Http.RavenCommand`1.SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RavenCommand.cs:line 129\r\n   at Raven.Client.Http.RequestExecutor.SendAsync[TResult](ServerNode chosenNode, RavenCommand`1 command, SessionInfo sessionInfo, HttpRequestMessage request, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1200\r\n   at Raven.Client.Http.RequestExecutor.SendRequestToServer[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, HttpRequestMessage request, String url, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1112\r\n   --- End of inner exception stack trace ---\r\n   at Raven.Client.Http.RequestExecutor.ThrowFailedToContactAllNodes[TResult](RavenCommand`1 command, HttpRequestMessage request) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1300\r\n   at Raven.Client.Http.RequestExecutor.SendRequestToServer[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, HttpRequestMessage request, String url, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1174\r\n   at Raven.Client.Http.RequestExecutor.ExecuteAsync[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 994\r\n   at Raven.Client.Http.RequestExecutor.HandleServerDown[TResult](String url, ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, HttpRequestMessage request, HttpResponseMessage response, Exception e, SessionInfo sessionInfo, Boolean shouldRetry, RequestContext requestContext, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1728\r\n   at Raven.Client.Http.RequestExecutor.SendRequestToServer[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, HttpRequestMessage request, String url, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 1170\r\n   at Raven.Client.Http.RequestExecutor.ExecuteAsync[TResult](ServerNode chosenNode, Nullable`1 nodeIndex, JsonOperationContext context, RavenCommand`1 command, Boolean shouldRetry, SessionInfo sessionInfo, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Client\\Http\\RequestExecutor.cs:line 994\r\n   at Raven.Server.Utils.ReplicationUtils.GetTcpInfoAsync(String url, GetTcpInfoCommand getTcpInfoCommand, X509Certificate2 certificate, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Server\\Utils\\ReplicationUtils.cs:line 45\r\n   at Raven.Server.Utils.ReplicationUtils.GetServerTcpInfoAsync(String url, String tag, X509Certificate2 certificate, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Server\\Utils\\ReplicationUtils.cs:line 31\r\n   at Raven.Server.ServerWide.ClusterStateMachine.ConnectToPeerAsync(String url, String tag, X509Certificate2 certificate, CancellationToken token) in C:\\workspaces\\ravendb\\src\\Raven.Server\\ServerWide\\ClusterStateMachine.cs:line 3968\r\n   --- End of inner exception stack trace ---\r\n   at System.Threading.Tasks.Task.ThrowIfExceptional(Boolean includeTaskCanceledExceptions)\r\n   at System.Threading.Tasks.Task.Wait(Int32 millisecondsTimeout, CancellationToken cancellationToken)\r\n   at System.Threading.Tasks.Task.Wait(Int32 millisecondsTimeout)\r\n   at Raven.Server.Rachis.FollowerAmbassador.WaitForConnection(Task`1 connectTask) in C:\\workspaces\\ravendb\\src\\Raven.Server\\Rachis\\FollowerAmbassador.cs:line 483\r\n   at Raven.Server.Rachis.FollowerAmbassador.Run(Object o) in C:\\workspaces\\ravendb\\src\\Raven.Server\\Rachis\\FollowerAmbassador.cs:line 189",
                    Destination: "C",
                    Connected: false,
                    Compression: false,
                    Features: {
                        BaseLine: true,
                        MultiTree: true,
                    },
                    StartAt: "2025-03-04T19:41:05.4361307Z",
                    LastSent: "2025-03-04T19:44:08.2198285Z",
                    LastReceived: "2025-03-04T19:44:08.0675124Z",
                } as any,
                {
                    Status: "Connected with A",
                    Destination: "A",
                    Connected: true,
                    Version: 54000,
                    Compression: false,
                    Features: {
                        BaseLine: true,
                        MultiTree: true,
                    },
                    StartAt: "2025-03-04T19:41:05.3354089Z",
                    LastSent: "2025-03-04T19:44:18.6847310Z",
                    LastReceived: "2025-03-04T19:44:18.6853907Z",
                } as any,
            ],
        };
    }
    static getClusterLogFollower(): Raven.Server.Rachis.FollowerDebugView {
        return {
            ...ManageServerStubs.getClusterLogBase(true),
            Role: "Follower",
            Phase: "Snapshot",
            ConnectionToLeader: {
                Status: "Connected",
                Destination: "A",
                Connected: true,
                Version: 54000,
                Compression: true,
                Features: {
                    BaseLine: true,
                    MultiTree: true,
                },
                StartAt: "2025-03-04T19:41:05.3354034Z",
                LastSent: "2025-03-04T19:44:18.6852884Z",
                LastReceived: "2025-03-04T19:44:18.6851199Z",
            } as any,
            RecentMessages: [
                {
                    At: "2025-03-04T19:44:18.7424518Z",
                    MsFromCycleStart: 0,
                    Message: "Wait for entries",
                },
                {
                    At: "2025-03-04T19:44:18.6853622Z",
                    MsFromCycleStart: 0,
                    Message: "Start",
                },
            ],
        };
    }

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

    static certificates(): CertificatesResponseDto {
        return {
            Certificates: [
                {
                    Name: "Server Certificate",
                    Thumbprint: "BCD2B71A3021A644E94768CCEFF7BE56E2006144",
                    SecurityClearance: "ClusterNode",
                    Permissions: {},
                    NotAfter: moment()
                        .add(5 as const, "years")
                        .format(),
                    NotBefore: moment()
                        .add(-10 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "SEZWHsvbycEsXVNFnj7a3Ou6r1B2xVmPQMhlmgw/NJc=",
                },
                {
                    Name: "Valid cert",
                    Thumbprint: "0F61904E1926ED2EDD5BB4BA8BC34742960B7839",
                    SecurityClearance: "ClusterAdmin",
                    Permissions: {},
                    NotAfter: moment()
                        .add(2 as const, "years")
                        .format(),
                    NotBefore: moment()
                        .add(-10 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "hyaqn9MDYitTWCf+oGwvu+GG9xqyxzZoZLANt5F/BL4=",
                    HasTwoFactor: false,
                },
                {
                    Name: "About to expire certAbout to expire certAbout to expire certAbout to expire certAbout to expire certAbout to expire certAbout to expire certAbout to expire cert",
                    Thumbprint: "05576326B5A2EC2CC59B4CDBFE51243ADC56187B",
                    SecurityClearance: "ValidUser",
                    Permissions: {
                        db2: "Read",
                        db1: "ReadWrite",
                    },
                    NotAfter: moment()
                        .add(5 as const, "days")
                        .format(),
                    NotBefore: moment()
                        .add(-10 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "FXoY7RVRnzcM8+m9ofo7IM5FnZp5SeDxHUOL74uzr+g=",
                    HasTwoFactor: true,
                },
                {
                    Name: "Expired cert",
                    Thumbprint: "6C19B1CD3171F10C55A7CC58E4E993D8524332B1",
                    SecurityClearance: "Operator",
                    Permissions: {},
                    NotAfter: moment()
                        .add(-5 as const, "days")
                        .format(),
                    NotBefore: moment()
                        .add(-20 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "tYDktnF7XEos5gOGMC4t4eBi5MDSAHDpFqX1rV9oLCE=",
                    HasTwoFactor: false,
                },
                {
                    Name: "Disabled cert",
                    Thumbprint: "7C19B1CD3171F10C55A7CC58E4E993D8524332B2",
                    SecurityClearance: "Operator",
                    Permissions: {},
                    NotAfter: moment()
                        .add(-5 as const, "days")
                        .format(),
                    NotBefore: moment()
                        .add(-20 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "uYDktnF7XEos5gOGMC4t4eBi5MDSAHDpFqX1rV9oLCE=",
                    HasTwoFactor: false,
                    Disabled: true,
                },
                {
                    Name: "Server Certificate for communication A",
                    Thumbprint: "JF61904E1926ED2EDD5BB4BA8BC34742960B7100",
                    SecurityClearance: "ClusterNode",
                    Permissions: {},
                    NotAfter: moment()
                        .add(2 as const, "years")
                        .format(),
                    NotBefore: moment()
                        .add(-10 as const, "days")
                        .format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "zyaqn9MDYitTWCf+oGwvu+GG9xqyxzZoZLANt5F/BL4=",
                    HasTwoFactor: false,
                },
            ],
            LoadedServerCert: "BCD2B71A3021A644E94768CCEFF7BE56E2006144",
            LoadedServerCertForCommunication: "JF61904E1926ED2EDD5BB4BA8BC34742960B7100",
            WellKnownAdminCerts: ["AdminCerts1", "AdminCerts2"],
            WellKnownIssuers: ["Issuers1", "Issuers2"],
        };
    }

    static certificatesWithSso(): CertificatesResponseDto {
        const ssoServerPinningHash1 = "SEZWHsvbycEsXVNFnj7a3Ou6r1B2xVmPQMhlmgw/NJc=";
        const ssoServerPinningHash2 = "hyaqn9MDYitTWCf+oGwvu+GG9xqyxzZoZLANt5F/BL4=";

        return {
            Certificates: [
                ...ManageServerStubs.certificates().Certificates,
                {
                    Name: "Corporate SSO Server",
                    Thumbprint: "AA11904E1926ED2EDD5BB4BA8BC34742960BAAA1",
                    SecurityClearance: "ValidUser",
                    Permissions: {},
                    NotAfter: moment().add(2 as const, "years").format(),
                    NotBefore: moment().add(-10 as const, "days").format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: ssoServerPinningHash1,
                    Usage: "SsoServer",
                    HasTwoFactor: false,
                },
                {
                    Name: "Backup SSO Server",
                    Thumbprint: "BB22904E1926ED2EDD5BB4BA8BC34742960BBBB2",
                    SecurityClearance: "ValidUser",
                    Permissions: {},
                    NotAfter: moment().add(1 as const, "years").format(),
                    NotBefore: moment().add(-5 as const, "days").format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: ssoServerPinningHash2,
                    Usage: "SsoServer",
                    HasTwoFactor: false,
                },
                {
                    Name: "SSO User: john@example.com",
                    Thumbprint: "CC33904E1926ED2EDD5BB4BA8BC34742960BCCC3",
                    SecurityClearance: "ValidUser",
                    Permissions: {
                        db1: "ReadWrite",
                    },
                    NotAfter: moment().add(2 as const, "years").format(),
                    NotBefore: moment().add(-10 as const, "days").format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "CCZWHsvbycEsXVNFnj7a3Ou6r1B2xVmPQMhlmgw/CC3=",
                    Usage: "SsoClient",
                    SsoServerPublicKeyPinningHashes: [ssoServerPinningHash1],
                    AllowAnySsoServer: false,
                    HasTwoFactor: false,
                },
                {
                    Name: "SSO User: alice@example.com",
                    Thumbprint: "DD44904E1926ED2EDD5BB4BA8BC34742960BDDD4",
                    SecurityClearance: "Operator",
                    Permissions: {},
                    NotAfter: moment().add(2 as const, "years").format(),
                    NotBefore: moment().add(-10 as const, "days").format(),
                    CollectionSecondaryKeys: [],
                    CollectionPrimaryKey: "",
                    PublicKeyPinningHash: "DDZWHsvbycEsXVNFnj7a3Ou6r1B2xVmPQMhlmgw/DD4=",
                    Usage: "SsoClient",
                    SsoServerPublicKeyPinningHashes: [],
                    AllowAnySsoServer: true,
                    HasTwoFactor: false,
                },
            ],
            LoadedServerCert: ManageServerStubs.certificates().LoadedServerCert,
            LoadedServerCertForCommunication: ManageServerStubs.certificates().LoadedServerCertForCommunication,
            WellKnownAdminCerts: ManageServerStubs.certificates().WellKnownAdminCerts,
            WellKnownIssuers: ManageServerStubs.certificates().WellKnownIssuers,
        };
    }

    static adminStats(): Raven.Server.ServerWide.ServerStatistics {
        return {
            LastRequestTime: moment().format(),
            LastAuthorizedNonClusterAdminRequestTime: null,
            LastRequestTimePerCertificate: {
                BCD2B71A3021A644E94768CCEFF7BE56E2006144: moment()
                    .add(-2 as const, "hours")
                    .format(),
                "0F61904E1926ED2EDD5BB4BA8BC34742960B7839": moment()
                    .add(-2 as const, "minutes")
                    .format(),
            },
        };
    }

    static serverCertificateRenewalDate(): string {
        return moment()
            .add(2 as const, "months")
            .format();
    }

    static serverCertificateSetupMode(): Raven.Server.Commercial.SetupMode {
        return "LetsEncrypt";
    }

    static twoFactorSecret(): { Secret: string } {
        return {
            Secret: "RNHRX6WXCLZVQPJSW5NSWV64JU65E5WA",
        };
    }
}
