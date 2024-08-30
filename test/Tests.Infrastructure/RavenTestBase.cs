using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract partial class RavenTestBase : TestBase
    {
        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected RavenTestBase(ITestOutputHelper output) : base(output)
        {
            Samples = new SamplesTestBase(this);
            TimeSeries = new TimeSeriesTestBase(this);
            Cluster = new ClusterTestBase2(this);
            Backup = new BackupTestBase(this);
            Encryption = new EncryptionTestBase(this);
            Certificates = new CertificatesTestBase(this);
            Indexes = new IndexesTestBase(this);
            Replication = new ReplicationTestBase2(this);
            Databases = new DatabasesTestBase(this);
        }

        private readonly object _getDocumentStoreSync = new object();

        protected virtual DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            try
            {
                lock (_getDocumentStoreSync)
                {
                    options ??= Options.Default;
                    var serverToUse = options.Server ?? Server;
                    AsyncHelpers.RunSync(() => serverToUse.ServerStore.EnsureNotPassiveAsync());

                    var name = GetDatabaseName(caller);

                    if (options.ModifyDatabaseName != null)
                        name = options.ModifyDatabaseName(name) ?? name;

                    var hardDelete = true;
                    var runInMemory = options.RunInMemory;

                    var pathToUse = options.Path;
                    if (runInMemory == false && options.ReplicationFactor > 1)
                    {
                        if (pathToUse == null)
                        {
                            // the folders will be assigned automatically
                        }
                        else
                        {
                            throw new InvalidOperationException($"You cannot set {nameof(Options)}.{nameof(Options.Path)} when, {nameof(Options)}.{nameof(Options.ReplicationFactor)} > 1 and {nameof(Options)}.{nameof(Options.RunInMemory)} == false.");
                        }
                    }
                    else if (pathToUse == null)
                    {
                        if (options.ReplicationFactor > 1)
                        {
                            // the folders will be assigned automatically - when running in cluster it's better to put files in directories under dedicated server / node dir
                        }
                        else
                        {
                            pathToUse = NewDataPath(name);
                        }
                    }
                    else
                    {
                        hardDelete = false;
                        runInMemory = false;
                    }

                    var doc = new DatabaseRecord(name)
                    {
                        Settings =
                        {
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString(),
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                            [RavenConfiguration.GetKey(x => x.Queries.RegexTimeout)] = (250).ToString()
                        }
                    };

                    if (options.Encrypted)
                        doc.Encrypted = true;

                    if (pathToUse != null)
                    {
                        doc.Settings.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), pathToUse);
                    }

                    options.ModifyDatabaseRecord?.Invoke(doc);

                    var store = new DocumentStore
                    {
                        Urls = UseFiddler(serverToUse.WebUrl),
                        Database = name,
                        Certificate = options.ClientCertificate,
                        Conventions =
                        {
                            DisableTopologyCache = true
                        }
                    };

                    options.ModifyDocumentStore?.Invoke(store);

                    //This gives too much error details in most cases, we don't need this now
                    store.RequestExecutorCreated += (sender, executor) =>
                    {
                        executor.AdditionalErrorInformation += sb => sb.AppendLine().Append(Cluster.GetLastStatesFromAllServersOrderedByTime());
                    };

                    store.Initialize();

                    if (options.CreateDatabase)
                    {
                        if (Servers.Contains(serverToUse))
                        {
                            Servers.ForEach(server => CheckIfDatabaseExists(server, name));
                        }
                        else
                        {
                            CheckIfDatabaseExists(serverToUse, name);
                        }

                        long raftCommand;
                        try
                        {
                            if (options.AdminCertificate != null)
                            {
                                using (var adminStore = new DocumentStore
                                {
                                    Urls = UseFiddler(serverToUse.WebUrl),
                                    Database = name,
                                    Certificate = options.AdminCertificate
                                }.Initialize())
                                {
                                    raftCommand = adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                                }
                            }
                            else
                            {
                                raftCommand = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                            }
                        }
                        catch (ConcurrencyException)
                        {
                            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(name));
                            Assert.Equal(options.ReplicationFactor, record.Topology.ReplicationFactor);
                            raftCommand = record.Etag;
                        }

                        Assert.True(raftCommand > 0); //sanity check

                        if (Servers.Contains(serverToUse))
                        {
                            var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                            AsyncHelpers.RunSync(async () => await Cluster.WaitForRaftIndexToBeAppliedInClusterWithNodesValidationAsync(raftCommand, timeout));

                            // skip 'wait for requests' on DocumentDatabase dispose
                            Servers.ForEach(server => ApplySkipDrainAllRequestsToDatabase(server, name));
                        }
                        else
                        {
                            ApplySkipDrainAllRequestsToDatabase(serverToUse, name);
                        }
                    }

                    store.BeforeDispose += (sender, args) =>
                    {
                        var realException = Context.GetException();
                        try
                        {
                            if (CreatedStores.TryRemove(store) == false)
                                return; // can happen if we are wrapping the store inside sharded one

                            DeleteDatabaseResult result = null;
                            if (options.DeleteDatabaseOnDispose)
                            {
                                result = DeleteDatabase(options, serverToUse, name, hardDelete, store);
                            }

                            if (Servers.Contains(serverToUse) && result != null)
                            {
                                var timeout = options.DeleteTimeout ?? TimeSpan.FromSeconds(Debugger.IsAttached ? 150 : 15);
                                AsyncHelpers.RunSync(async () => await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(result.RaftCommandIndex, timeout));
                            }
                        }
                        catch (Exception e)
                        {
                            if (realException != null)
                                throw new AggregateException(realException, e);

                            throw;
                        }
                    };
                    CreatedStores.Add(store);

                    return store;
                }
            }
            catch (TimeoutException te)
            {
                throw new TimeoutException($"{te.Message} {Environment.NewLine} {te.StackTrace}{Environment.NewLine}Servers states:{Environment.NewLine}{Cluster.GetLastStatesFromAllServersOrderedByTime()}");
            }
        }

        private static void CheckIfDatabaseExists(RavenServer server, string name)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                if (server.ServerStore.Cluster.Read(context, Constants.Documents.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }
        }

        private void ApplySkipDrainAllRequestsToDatabase(RavenServer serverToUse, string name)
        {
            try
            {
                var documentDatabase = AsyncHelpers.RunSync(() => serverToUse.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name));
                Assert.True(documentDatabase != null, $"(RavenDB-16924) documentDatabase is null on '{serverToUse.ServerStore.NodeTag}' {Environment.NewLine}{Cluster.CollectLogsFromNodes(Servers)}");
                documentDatabase.ForTestingPurposesOnly().SkipDrainAllRequests = true;
            }
            catch (DatabaseNotRelevantException)
            {
            }
        }

        private DeleteDatabaseResult DeleteDatabase(Options options, RavenServer serverToUse, string name, bool hardDelete, DocumentStore store)
        {
            try
            {
                if (options.AdminCertificate != null)
                {
                    using (var adminStore =
                        new DocumentStore { Urls = UseFiddler(serverToUse.WebUrl), Database = name, Certificate = options.AdminCertificate }.Initialize())
                    {
                        return adminStore.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
                    }
                }

                return store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
            }
            catch (OperationCanceledException)
            {
                //failed to delete in time
            }
            catch (TimeoutException)
            {
                //failed to delete in time
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (NoLeaderException)
            {
            }
            catch (AllTopologyNodesDownException)
            {

            }
            catch (Exception e)
            {
                if (e is RavenException && (e.InnerException is TimeoutException || e.InnerException is OperationCanceledException))
                    return null;

                if (Servers.Contains(serverToUse))
                {
                    if (Servers.All(s => s.Disposed))
                        return null;
                }

                if (serverToUse.Disposed)
                    return null;

                throw;
            }
            return null;
        }

        protected static async Task<TC> AssertWaitForSingleAsync<TC>(Func<Task<TC>> act, int timeout = 15000, int interval = 100) where TC : ICollection
        {
            var ret = await WaitForSingleAsync(act, timeout, interval);
            Assert.Single(ret);
            return ret;
        }
        protected static async Task<TC> AssertWaitForCountAsync<TC>(Func<Task<TC>> act, int count, int timeout = 15000, int interval = 100) where TC : ICollection
        {
            var ret = await WaitForCountAsync(act, count, timeout, interval);
            Assert.True(ret.Count == count, $"Expected {count}, Actual {ret.Count}");
            return ret;
        }

        protected static async Task<TC> WaitForSingleAsync<TC>(Func<Task<TC>> act, int timeout = 15000, int interval = 100) where TC : ICollection =>
            await WaitForCountAsync(act, 1, timeout, interval);
        protected static async Task<TC> WaitForCountAsync<TC>(Func<Task<TC>> act, int count, int timeout = 15000, int interval = 100) where TC : ICollection =>
            await WaitForPredicateAsync(a => a != null && a.Count == count, act, timeout, interval);

        protected static async Task<T> AssertWaitForGreaterThanAsync<T>(Func<Task<T>> act, T val, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var ret = await WaitForGreaterThanAsync(act, val, timeout, interval);
            if (ret.CompareTo(val) > 0 == false)
                throw new TimeoutException($"Timeout {TimeSpan.FromMilliseconds(timeout):g}. Value should be greater then {val}. Current value {ret}");
            return ret;
        }

        protected static async Task<T> WaitForGreaterThanAsync<T>(Func<Task<T>> act, T val, int timeout = 15000, int interval = 100) where T : IComparable =>
            await WaitForPredicateAsync(a => a.CompareTo(val) > 0, act, timeout, interval);

        protected static async Task AssertWaitForTrueAsync(Func<Task<bool>> act, int timeout = 15000, int interval = 100)
        {
            Assert.True(await WaitForValueAsync(act, true, timeout, interval));
        }

        protected static async Task<T> AssertWaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var ret = await WaitForValueAsync(act, expectedVal, timeout, interval);
            Assert.Equal(expectedVal, ret);
            return ret;
        }

        protected static async Task<T> WaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100) =>
             await WaitForPredicateAsync(a => (a == null && expectedVal == null) || (a != null && a.Equals(expectedVal)), act, timeout, interval);

        protected static async Task AssertWaitForExceptionAsync<T>(Func<Task> act, int timeout = 15000, int interval = 100)
            where T : class
        {
            await WaitAndAssertForValueAsync(async () =>
                await act().ContinueWith(t =>
                    t.Exception?.InnerException?.GetType()), typeof(T), timeout, interval);
        }

        protected static async Task<T> AssertWaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class
        {
            var ret = await WaitForNotNullAsync(act, timeout, interval);
            Assert.NotNull(ret);
            return ret;
        }

        protected static async Task<T> AssertWaitForNotDefaultAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100)
        {
            var ret = await WaitForNotDefaultAsync(act, timeout, interval);
            Assert.NotEqual(ret, default);
            return ret;
        }

        protected static async Task AssertWaitForNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class
        {
            var result = await WaitForNullAsync(act, timeout, interval);
            Assert.Null(result);
        }

        protected static async Task WaitAndAssertForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var val = await WaitForPredicateAsync(t =>
            {
                if (t == null)
                    return expectedVal == null;
                return t.Equals(expectedVal);
            }, act, timeout, interval);
            Assert.Equal(expectedVal, val);
        }

        protected static async Task<T> AssertWaitForGreaterAsync<T>(Func<T> act, T value, int timeout = 15000, int interval = 100) where T : IComparable
        {
            return await AssertWaitForGreaterAsync(() => Task.FromResult(act()), value, timeout, interval);
        }

        protected static async Task<T> AssertWaitForGreaterAsync<T>(Func<Task<T>> act, T value, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var ret = await WaitForPredicateAsync(r => r.CompareTo(value) > 0, act, timeout, interval);
            Assert.NotNull(ret);
            return ret;
        }

        protected static async Task<T> WaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class =>
            await WaitForPredicateAsync(a => a != null, act, timeout, interval);

        protected static async Task<T> WaitForNotDefaultAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) =>
            await WaitForPredicateAsync(a => !EqualityComparer<T>.Default.Equals(a, default), act, timeout, interval);

        protected static async Task<T> WaitForNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class =>
            await WaitForPredicateAsync(a => a == null, act, timeout, interval);

        protected static async Task<T> WaitAndAssertForGreaterThanAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100) where T : IComparable
        {
            var actualValue = await WaitForGreaterThanAsync(act, expectedVal, timeout, interval);
            Assert.True(actualValue.CompareTo(expectedVal) > 0, $"expectedVal:{expectedVal}, actualValue: {actualValue}");
            return actualValue;
        }

        protected async Task WaitAndAssertForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var val = await WaitForPredicateAsync(t => t.Equals(expectedVal), () => Task.FromResult(act.Invoke()), timeout, interval);
            Assert.Equal(expectedVal, val);
        }

        protected static async Task<T> WaitForPredicateAsync<T>(Predicate<T> predicate, Func<Task<T>> act, int timeout = 15000, int interval = 100)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    var currentVal = await act();
                    if (predicate(currentVal) || sw.ElapsedMilliseconds > timeout)
                        return currentVal;
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(interval);
            }
        }

        protected static async Task<T> WaitForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(100);
            } while (true);
        }

        protected static T WaitForValue<T>(Func<T> act, T expectedVal, int timeout = 15000, int interval = 16)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }

                Thread.Sleep(interval);
            } while (true);
        }

        public static void WaitForUserToContinueTheTest(string url, bool debug = true, X509Certificate2 clientCert = null)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            RavenTestHelper.AssertNotRunningOnCi();

            if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    userPersonalStore.Open(OpenFlags.ReadWrite);
                    userPersonalStore.Add(clientCert);
                }
            }

            try
            {
                var documentsPage = url + "/studio/index.html";

                OpenBrowser(documentsPage);// start the server

                do
                {
                    Thread.Sleep(500);
                } while (debug == false || Debugger.IsAttached);
            }
            finally
            {
                if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                    {
                        userPersonalStore.Open(OpenFlags.ReadWrite);
                        userPersonalStore.Remove(clientCert);
                    }
                }
            }
        }

        public static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null, X509Certificate2 clientCert = null)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            RavenTestHelper.AssertNotRunningOnCi();

            if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    userPersonalStore.Open(OpenFlags.ReadWrite);
                    userPersonalStore.Add(clientCert);
                }
            }

            try
            {
                var urls = documentStore.Urls;
                if (clientCert != null)
                    Console.WriteLine($"Using certificate with serial: {clientCert.SerialNumber}");

                var databaseNameEncoded = Uri.EscapeDataString(database ?? documentStore.Database);
                var documentsPage = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true&disableAnalytics=true";

                OpenBrowser(documentsPage);// start the server

                do
                {
                    Thread.Sleep(500);
                } while (documentStore.Commands(database).Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));

                documentStore.Commands(database).Delete("Debug/Done", null);
            }
            finally
            {
                if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                    {
                        userPersonalStore.Open(OpenFlags.ReadWrite);
                        userPersonalStore.Remove(clientCert);
                    }
                }
            }
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
            {
                if (store.WasDisposed)
                    continue;

                exceptionAggregator.Execute(store.Dispose);
            }
            CreatedStores.Clear();
        }

        public class Options
        {
            private readonly bool _frozen;

            private X509Certificate2 _clientCertificate;
            private X509Certificate2 _adminCertificate;
            private bool _createDatabase;
            private bool _deleteDatabaseOnDispose;
            private TimeSpan? _deleteTimeout;
            private RavenServer _server;
            private int _replicationFactor;
            private bool _ignoreDisabledDatabase;
            private Action<DocumentStore> _modifyDocumentStore;
            private Action<DatabaseRecord> _modifyDatabaseRecord;
            private Func<string, string> _modifyDatabaseName;
            private string _path;
            private bool _runInMemory = true;
            private bool _encrypted;

            private StringBuilder _descriptionBuilder;

            public static readonly Options Default = new Options(true);

            public Options() : this(false)
            {
            }

            public static Options ForSearchEngine(RavenSearchEngineMode mode)
            {
                var config = new RavenTestParameters() {SearchEngine = mode};
                return ForSearchEngine(config);
            }


            public static Options ForSearchEngine(RavenTestParameters config)
            {
                return new Options()
                {
                    ModifyDatabaseRecord = d =>
                    {
                        d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                        d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                    }
                };
            }

            private Options(bool frozen)
            {
                DeleteDatabaseOnDispose = true;
                CreateDatabase = true;
                ReplicationFactor = 1;

                _frozen = frozen;
            }

            public static Options ForMode(RavenDatabaseMode mode)
            {
                var options = new Options();
                switch (mode)
                {
                    case RavenDatabaseMode.Single:
                        options.DatabaseMode = RavenDatabaseMode.Single;
                        options.AddToDescription($"{nameof(RavenDataAttribute.DatabaseMode)} = {nameof(RavenDatabaseMode.Single)}");
                        return options;
                    case RavenDatabaseMode.All:
                    default:
                        throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
                }
            }

            internal void AddToDescription(string descriptionToAdd)
            {
                _descriptionBuilder ??= new StringBuilder();

                _descriptionBuilder
                    .Append(" ")
                    .Append(descriptionToAdd);
            }

            public string Path
            {
                get => _path;
                set
                {
                    AssertNotFrozen();
                    _path = value;
                }
            }

            public Func<string, string> ModifyDatabaseName
            {
                get => _modifyDatabaseName;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseName = value;
                }
            }

            public Action<DatabaseRecord> ModifyDatabaseRecord
            {
                get => _modifyDatabaseRecord;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseRecord = value;
                }
            }

            public Action<DocumentStore> ModifyDocumentStore
            {
                get => _modifyDocumentStore;
                set
                {
                    AssertNotFrozen();
                    _modifyDocumentStore = value;
                }
            }

            public bool IgnoreDisabledDatabase
            {
                get => _ignoreDisabledDatabase;
                set
                {
                    AssertNotFrozen();
                    _ignoreDisabledDatabase = value;
                }
            }

            public int ReplicationFactor
            {
                get => _replicationFactor;
                set
                {
                    AssertNotFrozen();
                    _replicationFactor = value;
                }
            }

            public RavenServer Server
            {
                get => _server;
                set
                {
                    AssertNotFrozen();
                    _server = value;
                }
            }

            public bool DeleteDatabaseOnDispose
            {
                get => _deleteDatabaseOnDispose;
                set
                {
                    AssertNotFrozen();
                    _deleteDatabaseOnDispose = value;
                }
            }

            public TimeSpan? DeleteTimeout
            {
                get => _deleteTimeout;
                set
                {
                    AssertNotFrozen();
                    _deleteTimeout = value;
                }
            }

            public bool CreateDatabase
            {
                get => _createDatabase;
                set
                {
                    AssertNotFrozen();
                    _createDatabase = value;
                    if (value == false)
                    {
                        ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true;
                    }
                }
            }

            public bool RunInMemory
            {
                get => _runInMemory;
                set
                {
                    AssertNotFrozen();
                    _runInMemory = value;
                }
            }

            public X509Certificate2 AdminCertificate
            {
                get => _adminCertificate;
                set
                {
                    AssertNotFrozen();
                    _adminCertificate = value;
                }
            }

            public X509Certificate2 ClientCertificate
            {
                get => _clientCertificate;
                set
                {
                    AssertNotFrozen();
                    _clientCertificate = value;
                }
            }

            public bool Encrypted
            {
                get => _encrypted;
                set
                {
                    AssertNotFrozen();
                    _encrypted = value;
                }
            }

            public RavenDatabaseMode DatabaseMode { get; private set; }

            public RavenSearchEngineMode SearchEngineMode { get; internal set; }

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("Options are frozen and cannot be changed.");
            }

            public override string ToString()
            {
                return _descriptionBuilder == null
                    ? base.ToString()
                    : _descriptionBuilder.ToString();
            }

            public Options Clone()
            {
                return new Options
                {
                    AdminCertificate = AdminCertificate,
                    ClientCertificate = ClientCertificate,
                    CreateDatabase = CreateDatabase,
                    DeleteDatabaseOnDispose = DeleteDatabaseOnDispose,
                    DeleteTimeout = DeleteTimeout,
                    Encrypted = Encrypted,
                    IgnoreDisabledDatabase = IgnoreDisabledDatabase,
                    ModifyDatabaseName = ModifyDatabaseName,
                    ModifyDatabaseRecord = ModifyDatabaseRecord,
                    ModifyDocumentStore = ModifyDocumentStore,
                    Path = Path,
                    ReplicationFactor = ReplicationFactor,
                    RunInMemory = RunInMemory,
                    Server = Server,
                    _descriptionBuilder = new StringBuilder(_descriptionBuilder.ToString())
                };
            }
        }

        public int GetAvailablePort()
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var port = ((IPEndPoint)tcpListener.LocalEndpoint).Port;
            tcpListener.Stop();

            return port;
        }

        public static string GenRandomString(int size)
        {
            return GenRandomString(new Random(), size);
        }

        public static string GenRandomString(Random random, int size)
        {
            var sb = new StringBuilder(size);
            // var ran = new Random();
            var firstCharAsInt = Convert.ToInt32('a');
            var lastCharAsInt = Convert.ToInt32('z');
            for (int i = 0; i < size; i++)
            {
                sb.Append(Convert.ToChar(random.Next(firstCharAsInt, lastCharAsInt + 1)));
            }

            return sb.ToString();
        }

        public static async Task<string> ReadFromWebSocket(ArraySegment<byte> buffer, WebSocket source)
        {
            using (var ms = new MemoryStream())
            {
                WebSocketReceiveResult result;
                do
                {
                    try
                    {
                        result = await source.ReceiveAsync(buffer, CancellationToken.None);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                    ms.Write(buffer.Array, buffer.Offset, result.Count);
                }
                while (!result.EndOfMessage);
                ms.Seek(0, SeekOrigin.Begin);

                return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
            }
        }
    }
}
