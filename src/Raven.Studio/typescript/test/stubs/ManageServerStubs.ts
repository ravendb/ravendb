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
