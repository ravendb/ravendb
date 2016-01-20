using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Common;
using Raven.Database.Counters;
using Raven.Database.FileSystem;
using Raven.Database.Impl;
using Raven.Database.Plugins.Builtins;
using Raven.Database.Queries;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Tenancy;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Server.WebApi
{
    public class RequestManager : IDisposable
    {

        private readonly DateTime startUpTime = SystemTime.UtcNow;
        private readonly DatabasesLandlord landlord;
        public DateTime StartUpTime
        {
            get { return startUpTime; }
        }

        private long reqNum;
        private Timer serverTimer;
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
        private readonly TimeSpan maxTimeDatabaseCanBeIdle;
        private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
        private readonly ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>> tracedRequests =
            new ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>>();

        private readonly CancellationTokenSource cancellationTokenSource;
        private long concurrentRequests;
        private int physicalRequestsCount;
        private bool initialized;
        private CancellationToken cancellationToken;

        private ConcurrentDictionary<string, ConcurrentSet<IEventsTransport>> resourceHttpTraces = new ConcurrentDictionary<string, ConcurrentSet<IEventsTransport>>();
        private readonly ConcurrentSet<IEventsTransport> serverHttpTrace = new ConcurrentSet<IEventsTransport>();

        public int NumberOfRequests
        {
            get { return Thread.VolatileRead(ref physicalRequestsCount); }
        }

        public long NumberOfConcurrentRequests
        {
            get { return Thread.VolatileRead(ref concurrentRequests); }
        }
        public HotSpareReplicationBehavior HotSpareValidator { get; set; }

        public event EventHandler<RequestWebApiEventArgs> BeforeRequest;
        public event EventHandler<RequestWebApiEventArgs> AfterRequest;

        public DateTime? LastRequestTime { get; private set; }

        public virtual void OnBeforeRequest(RequestWebApiEventArgs e)
        {
            var handler = BeforeRequest;
            if (handler != null) handler(this, e);
        }

        public virtual void OnAfterRequest(RequestWebApiEventArgs e)
        {
            var handler = AfterRequest;
            if (handler != null) handler(this, e);
        }

        public RequestManager(DatabasesLandlord landlord)
        {
            BeforeRequest += OnBeforeRequest;
            AfterRequest += OnAfterRequest;
            cancellationTokenSource = new CancellationTokenSource();
            this.landlord = landlord;

            maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(landlord.MaxIdleTimeForTenantDatabaseInSec);
            frequencyToCheckForIdleDatabases = TimeSpan.FromSeconds(landlord.FrequencyToCheckForIdleDatabasesInSec);

            Init();
            cancellationToken = cancellationTokenSource.Token;
        }


        private void OnBeforeRequest(object sender, RequestWebApiEventArgs args)
        {
            if (args.ResourceType == ResourceType.Database)
            {
                var documentDatabase = (DocumentDatabase)args.Resource;
                documentDatabase.WorkContext.MetricsCounters.ConcurrentRequests.Mark();
                documentDatabase.WorkContext.MetricsCounters.RequestsPerSecondCounter.Mark();
                Interlocked.Increment(ref documentDatabase.WorkContext.MetricsCounters.ConcurrentRequestsCount);
                return;
            }

            if (args.ResourceType == ResourceType.FileSystem)
            {
                var fileSystem = (RavenFileSystem)args.Resource;
                fileSystem.MetricsCounters.ConcurrentRequests.Mark();
                fileSystem.MetricsCounters.RequestsPerSecondCounter.Mark();
                Interlocked.Increment(ref fileSystem.MetricsCounters.ConcurrentRequestsCount);
                return;
            }

            if (args.ResourceType == ResourceType.Counter)
            {
                var counters = (CounterStorage)args.Resource;
                counters.MetricsCounters.RequestsPerSecondCounter.Mark();
                Interlocked.Increment(ref counters.MetricsCounters.ConcurrentRequestsCount);
            }
        }

        private void OnAfterRequest(object sender, RequestWebApiEventArgs args)
        {
            if (args.ResourceType == ResourceType.Database)
            {
                var documentDatabase = (DocumentDatabase)args.Resource;
                Interlocked.Decrement(ref documentDatabase.WorkContext.MetricsCounters.ConcurrentRequestsCount);
                return;
            }

            if (args.ResourceType == ResourceType.FileSystem)
            {
                var fileSystem = (RavenFileSystem)args.Resource;
                Interlocked.Decrement(ref fileSystem.MetricsCounters.ConcurrentRequestsCount);
                return;
            }

            if (args.ResourceType == ResourceType.Counter)
            {
                var counters = (CounterStorage)args.Resource;
                Interlocked.Decrement(ref counters.MetricsCounters.ConcurrentRequestsCount);
            }
        }

        public void Init()
        {
            if (initialized)
                return;

            initialized = true;
            serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
        }


        public void Dispose()
        {

            cancellationTokenSource.Cancel();
            // we give it a second to actually complete all requests, but then we go ahead 
            // and dispose anyway
            for (int i = 0; i < 10 && Interlocked.Read(ref concurrentRequests) > 0; i++)
            {
                Thread.Sleep(100);
            }

            var exceptionAggregator = new ExceptionAggregator(Logger, "Could not properly dispose of HttpServer");

            exceptionAggregator.Execute(() =>
            {
                if (serverTimer != null)
                    serverTimer.Dispose();
            });
        }



        public async Task<HttpResponseMessage> HandleActualRequest(IResourceApiController controller,
                                                                   HttpControllerContext controllerContext,
                                                                   Func<Task<HttpResponseMessage>> action,
                                                                   Func<HttpException, HttpResponseMessage> onHttpException)
        {

            HttpResponseMessage response = null;
            cancellationToken.ThrowIfCancellationRequested();

            Stopwatch sw = Stopwatch.StartNew();

            try
            {
                LastRequestTime = SystemTime.UtcNow;
                Interlocked.Increment(ref concurrentRequests);

                IncrementRequestNumberAndLog(controller, controllerContext);

                RequestWebApiEventArgs args = await controller.TrySetupRequestToProperResource().ConfigureAwait(false);
                if (args != null)
                {
                    OnBeforeRequest(args);

                    try
                    {
                        if (controllerContext.Request.Headers.Contains(Constants.RavenClientVersion))
                        {
                            if (controller.RejectClientRequests)
                            {
                                response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                                {
                                    Content = new MultiGetSafeStringContent("This service is not accepting clients calls")
                                };
                            }
                            else if (IsInHotSpareMode)
                            {
                                response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                                {
                                    Content = new MultiGetSafeStringContent("This service is not accepting clients calls because this is a 'Hot Spare' server")
                                };
                            }
                            else
                            {

                                response = await action().ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            response = await action().ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        OnAfterRequest(args);
                    }
                }
            }
            catch (HttpException httpException)
            {
                response = onHttpException(httpException);
            }
            finally
            {

                Interlocked.Decrement(ref concurrentRequests);
                try
                {
                    FinalizeRequestProcessing(controller, response, sw);
                }
                catch (Exception e)
                {

                    var aggregateException = e as AggregateException;
                    if (aggregateException != null)
                    {

                        e = aggregateException.ExtractSingleInnerException();
                    }

                    Logger.ErrorException("Could not finalize request properly", e);
                }
            }
            return response;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void IncrementRequestNumberAndLog(IResourceApiController controller, HttpControllerContext controllerContext)
        {
            if (controller is StudioController || controller is HardRouteController || controller is SilverlightController)
                return;

            var curReq = Interlocked.Increment(ref reqNum);
            controllerContext.Request.Properties["requestNum"] = curReq;

            if (Logger.IsDebugEnabled)
            {
                Logger.Debug(string.Format(CultureInfo.InvariantCulture,
                    "Receive Request #{0,4:#,0}: {1,-7} - {2} - {3}",
                    curReq,
                    controller.InnerRequest.Method.Method,
                    controller.ResourceName,
                    controller.InnerRequest.RequestUri));
            }
        }

        /// <summary>
        /// If set all client request to the server will be rejected with 
        /// the http 503 response.
        /// And no replication can be done from this database.
        /// </summary>

        public bool IsInHotSpareMode { get; set; }


        // Cross-Origin Resource Sharing (CORS) is documented here: http://www.w3.org/TR/cors/
        public void AddAccessControlHeaders(RavenBaseApiController controller, HttpResponseMessage msg)
        {

            var accessControlAllowOrigin = landlord.SystemConfiguration.AccessControlAllowOrigin;
            if (accessControlAllowOrigin.Count == 0)
                return;

            var originHeader = controller.GetHeader("Origin");
            if (originHeader == null || originHeader.Contains(controller.InnerRequest.Headers.Host))
                return; // no need

            bool originAllowed = accessControlAllowOrigin.Contains("*") ||
                                 accessControlAllowOrigin.Contains(originHeader);
            if (originAllowed)
            {
                controller.AddHeader("Access-Control-Allow-Origin", originHeader, msg);
            }
            if (controller.InnerRequest.Method.Method != "OPTIONS")
                return;

            controller.AddHeader("Access-Control-Allow-Credentials", "true", msg);
            controller.AddHeader("Access-Control-Max-Age", landlord.SystemConfiguration.AccessControlMaxAge, msg);
            controller.AddHeader("Access-Control-Allow-Methods", landlord.SystemConfiguration.AccessControlAllowMethods, msg);
            if (string.IsNullOrEmpty(landlord.SystemConfiguration.AccessControlRequestHeaders))
            {

                // allow whatever headers are being requested
                var hdr = controller.GetHeader("Access-Control-Request-Headers"); // typically: "x-requested-with"
                if (hdr != null)
                    controller.AddHeader("Access-Control-Allow-Headers", hdr, msg);
            }
            else
            {
                controller.AddHeader("Access-Control-Request-Headers", landlord.SystemConfiguration.AccessControlRequestHeaders, msg);
            }
        }



        public void SetThreadLocalState(IEnumerable<KeyValuePair<string, IEnumerable<string>>> innerHeaders, string databaseName)
        {
            CurrentOperationContext.Headers.Value = new Lazy<NameValueCollection>(() =>
            {
                var nameValueCollection = new NameValueCollection();
                foreach (var innerHeader in innerHeaders)
                {
                    nameValueCollection[innerHeader.Key] = innerHeader.Value.FirstOrDefault();
                }

                return nameValueCollection;
            });

            CurrentOperationContext.User.Value = null;

            LogContext.ResourceName = databaseName;
            var disposable = LogManager.OpenMappedContext("database", databaseName ?? Constants.SystemDatabase);

            CurrentOperationContext.RequestDisposables.Value.Add(disposable);
        }

        public void ResetThreadLocalState()
        {
            try
            {
                CurrentOperationContext.Headers.Value = null;
                CurrentOperationContext.User.Value = null;
                LogContext.ResourceName = null;
                foreach (var disposable in CurrentOperationContext.RequestDisposables.Value)
                {

                    disposable.Dispose();
                }

                CurrentOperationContext.RequestDisposables.Value.Clear();
            }
            catch
            {
                // this can happen during system shutdown
            }
        }

        public void ResetNumberOfRequests()
        {
            Interlocked.Exchange(ref reqNum, 0);
            Interlocked.Exchange(ref physicalRequestsCount, 0);
        }


        public void IncrementRequestCount()
        {
            Interlocked.Increment(ref physicalRequestsCount);
        }


        private void FinalizeRequestProcessing(IResourceApiController controller, HttpResponseMessage response, Stopwatch sw)
        {
            LogHttpRequestStatsParams logHttpRequestStatsParam = null;
            try
            {
                StringBuilder sb = null;
                if (controller.CustomRequestTraceInfo != null && controller.CustomRequestTraceInfo.Count > 0)
                {

                    sb = new StringBuilder();
                    foreach (var action in controller.CustomRequestTraceInfo)
                    {
                        try
                        {
                            action(sb);
                        }
                        catch (Exception e)
                        {
                            Logger.WarnException("Could not gather information to log request stats custom info, so far got " + sb, e);
                        }
                        sb.AppendLine();
                    }
                    while (sb.Length > 0)
                    {

                        if (!char.IsWhiteSpace(sb[sb.Length - 1]))
                            break;
                        sb.Length--;
                    }
                }
                var innerRequest = controller.InnerRequest;
                var httpRequestHeaders = innerRequest.Headers;
                var httpContentHeaders = innerRequest.Content == null ? null : innerRequest.Content.Headers;
                logHttpRequestStatsParam = new LogHttpRequestStatsParams(
                    sw,
                    new Lazy<HttpHeaders>(() => RavenBaseApiController.CloneRequestHttpHeaders(httpRequestHeaders, httpContentHeaders)),
                    controller.InnerRequest.Method.Method,
                    response != null ? (int)response.StatusCode : 500,
                    controller.InnerRequest.RequestUri.ToString(),
                    sb != null ? sb.ToString() : null,
                    controller.InnerRequestsCount
                    );
            }
            catch (Exception e)
            {

                Logger.WarnException("Could not gather information to log request stats", e);
            }

            if (logHttpRequestStatsParam == null || sw == null)
                return;

            sw.Stop();

            MarkRequestDuration(controller, sw.ElapsedMilliseconds);

            object requestNumber;

            if (controller.InnerRequest.Properties.TryGetValue("requestNum", out requestNumber) && requestNumber is long)
            {
                LogHttpRequestStats(controller, logHttpRequestStatsParam, controller.ResourceName, (long)requestNumber);
            }

            if (controller.IsInternalRequest == false)
            {
                TraceTraffic(controller, logHttpRequestStatsParam, controller.ResourceName, response);
            }

            RememberRecentRequests(logHttpRequestStatsParam, controller.ResourceName);
        }

        private void MarkRequestDuration(IResourceApiController controller, long elapsedMilliseconds)
        {
            switch (controller.ResourceType)
            {
                case ResourceType.Database:
                    if (landlord.IsDatabaseLoaded(controller.ResourceName ?? Constants.SystemDatabase))
                        controller.MarkRequestDuration(elapsedMilliseconds);
                    break;
                case ResourceType.FileSystem:
                    break;
                case ResourceType.TimeSeries:
                    break;
                case ResourceType.Counter:
                    break;
                default:
                    throw new NotSupportedException(controller.ResourceType.ToString());
            }
        }

        private void RememberRecentRequests(LogHttpRequestStatsParams requestLog, string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = Constants.SystemDatabase;

            var traces = tracedRequests.GetOrAdd(databaseName, new ConcurrentQueue<LogHttpRequestStatsParams>());

            LogHttpRequestStatsParams _;
            while (traces.Count > 50 && traces.TryDequeue(out _))
            {
            }

            traces.Enqueue(requestLog);
        }


        public IEnumerable<LogHttpRequestStatsParams> GetRecentRequests(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = Constants.SystemDatabase;

            ConcurrentQueue<LogHttpRequestStatsParams> queue;
            if (tracedRequests.TryGetValue(databaseName, out queue) == false)
                return Enumerable.Empty<LogHttpRequestStatsParams>();

            return queue.ToArray().Reverse();
        }

        private void TraceTraffic(IResourceApiController controller, LogHttpRequestStatsParams logHttpRequestStatsParams, string resourceName, HttpResponseMessage response)
        {
            if (HasAnyHttpTraceEventTransport() == false)
                return;

            RavenJObject timingsJson = null;

            if (controller is IndexController)
            {
                var jsonContent = response.Content as JsonContent;
                if (jsonContent != null && jsonContent.Data != null)
                {
                    timingsJson = jsonContent.Data.Value<RavenJObject>("TimingsInMilliseconds");
                }
            }

            NotifyTrafficWatch(
                string.IsNullOrEmpty(resourceName) == false ? resourceName : Constants.SystemDatabase,
                new TrafficWatchNotification()
                {
                    RequestUri = logHttpRequestStatsParams.RequestUri,
                    ElapsedMilliseconds = logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds,
                    CustomInfo = logHttpRequestStatsParams.CustomInfo,
                    HttpMethod = logHttpRequestStatsParams.HttpMethod,
                    ResponseStatusCode = logHttpRequestStatsParams.ResponseStatusCode,
                    TenantName = NormalizeTennantName(resourceName),
                    TimeStamp = SystemTime.UtcNow,
                    InnerRequestsCount = logHttpRequestStatsParams.InnerRequestsCount,
                    QueryTimings = timingsJson
                }
            );
        }


        private string NormalizeTennantName(string resourceName)
        {
            if (string.IsNullOrEmpty(resourceName))
            {
                return "<system>";
            }

            if (resourceName.StartsWith("fs/", StringComparison.OrdinalIgnoreCase) ||
                resourceName.StartsWith("cs/", StringComparison.OrdinalIgnoreCase) ||
                resourceName.StartsWith("ts/", StringComparison.OrdinalIgnoreCase))
            {
                return resourceName.Substring(3);
            }

            return resourceName;
        }
        private void LogHttpRequestStats(IResourceApiController controller, LogHttpRequestStatsParams logHttpRequestStatsParams, string databaseName, long curReq)
        {
            if (Logger.IsDebugEnabled == false)
                return;

            if (controller is StudioController || controller is HardRouteController || controller is SilverlightController)
                return;

            var message = string.Format(CultureInfo.InvariantCulture, "Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {5,-10} - {3} - {4}",
                curReq,
                logHttpRequestStatsParams.HttpMethod,
                logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds,
                logHttpRequestStatsParams.ResponseStatusCode,
                logHttpRequestStatsParams.RequestUri,
                databaseName);
            if (Logger.IsDebugEnabled)
                Logger.Debug(message);
            if (Logger.IsDebugEnabled && string.IsNullOrWhiteSpace(logHttpRequestStatsParams.CustomInfo) == false)
            {
                Logger.Debug(logHttpRequestStatsParams.CustomInfo);
            }
        }

        private bool HasAnyHttpTraceEventTransport()
        {
            return serverHttpTrace.Count > 0 || resourceHttpTraces.Count > 0;
        }

        private void NotifyTrafficWatch(string resourceName, TrafficWatchNotification trafficWatchNotification)
        {
            object notificationMessage = new
            {
                Type = "LogNotification",
                Value = trafficWatchNotification
            };

            if (serverHttpTrace.Count > 0)
            {
                foreach (var eventsTransport in serverHttpTrace)
                {
                    eventsTransport.SendAsync(notificationMessage);
                }
            }

            if (resourceHttpTraces.Count > 0)
            {
                ConcurrentSet<IEventsTransport> resourceEventTransports;

                if (!resourceHttpTraces.TryGetValue(resourceName, out resourceEventTransports) || resourceEventTransports.Count == 0)
                    return;

                foreach (var eventTransport in resourceEventTransports)
                {
                    eventTransport.SendAsync(notificationMessage);
                }
            }
        }

        public void RegisterServerHttpTraceTransport(IEventsTransport transport)
        {
            serverHttpTrace.Add(transport);
            transport.Disconnected += () => serverHttpTrace.TryRemove(transport);
        }

        public void RegisterResourceHttpTraceTransport(IEventsTransport transport, string resourceName)
        {
            var curResourceEventTransports = resourceHttpTraces.GetOrAdd(resourceName);
            curResourceEventTransports.Add(transport);
            transport.Disconnected += () =>
            {
                ConcurrentSet<IEventsTransport> resourceEventTransports;
                resourceHttpTraces.TryGetValue(resourceName, out resourceEventTransports);

                if (resourceEventTransports != null)
                    resourceEventTransports.TryRemove(transport);
            };
        }


        private void IdleOperations(object state)
        {
            try
            {
                try
                {
                    if (DatabaseNeedToRunIdleOperations(landlord.SystemDatabase))
                        landlord.SystemDatabase.RunIdleOperations();
                }
                catch (Exception e)
                {
                    Logger.ErrorException("Error during idle operation run for system database", e);
                }


                foreach (var documentDatabase in landlord.ResourcesStoresCache)
                {
                    try
                    {
                        if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = documentDatabase.Value.Result;
                        if (DatabaseNeedToRunIdleOperations(database))
                            database.RunIdleOperations();
                    }

                    catch (Exception e)
                    {

                        Logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
                    }
                }



                var databasesToCleanup = landlord.LastRecentlyUsed
                    .Where(x => (SystemTime.UtcNow - x.Value) > maxTimeDatabaseCanBeIdle)
                    .Select(x => x.Key)
                    .ToArray();

                foreach (var db in databasesToCleanup)
                {
                    // intentionally inside the loop, so we get better concurrency overall
                    // since shutting down a database can take a while
                    landlord.Cleanup(db, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle, shouldSkip: database => database.Configuration.RunInMemory);
                }

                FacetedQueryRunner.IntArraysPool.Instance.RunIdleOperations();
            }
            catch (Exception e)
            {
                Logger.WarnException("Error during idle operations for the server", e);
            }
            finally
            {
                try
                {
                    serverTimer.Change(frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private bool DatabaseNeedToRunIdleOperations(DocumentDatabase documentDatabase)
        {
            var dateTime = SystemTime.UtcNow;
            if ((dateTime - documentDatabase.WorkContext.LastWorkTime).TotalMinutes > 5)
                return true;
            if ((dateTime - documentDatabase.WorkContext.LastIdleTime).TotalHours > 2)
                return true;
            return false;
        }
    }
}
