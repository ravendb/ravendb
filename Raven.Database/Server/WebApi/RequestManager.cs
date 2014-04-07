using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.VisualBasic.Logging;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;

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
	    private int reqNum;
		private Timer serverTimer;
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private readonly ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>> tracedRequests =
			new ConcurrentDictionary<string, ConcurrentQueue<LogHttpRequestStatsParams>>();

		private DateTime lastWriteRequest;
	    private readonly CancellationTokenSource cancellationTokenSource;
	    private long concurrentRequests;
		private int physicalRequestsCount;
		private bool initialized;
	    private CancellationToken cancellationToken;

	    public int NumberOfRequests
		{
			get { return Thread.VolatileRead(ref physicalRequestsCount); }
		}

		public event EventHandler<BeforeRequestWebApiEventArgs> BeforeRequest;

	    public virtual void OnBeforeRequest(BeforeRequestWebApiEventArgs e)
	    {
	        var handler = BeforeRequest;
	        if (handler != null) handler(this, e);
	    }

	    public RequestManager(DatabasesLandlord landlord)
		{
            BeforeRequest+=OnBeforeRequest;
		    cancellationTokenSource = new CancellationTokenSource();
		    this.landlord = landlord;
			int val;
			if (int.TryParse(landlord.SystemConfiguration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;
			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val);

			if (int.TryParse(landlord.SystemConfiguration.Settings["Raven/Tenants/FrequencyToCheckForIdleDatabases"], out val) == false)
				val = 60;
			frequencyToCheckForIdleDatabases = TimeSpan.FromSeconds(val);

			Init();
		    cancellationToken = cancellationTokenSource.Token;
		}

	    private void OnBeforeRequest(object sender, BeforeRequestWebApiEventArgs args)
	    {
	        var documentDatabase = args.Database;
	        if (documentDatabase != null)
	        {
                documentDatabase.WorkContext.MetricsCounters.ConcurrentRequests.Mark();
                documentDatabase.WorkContext.MetricsCounters.RequestsPerSecondCounter.Mark();
	        }

	        var fileSystem = args.FileSystem;
            if (fileSystem != null)
            {
                fileSystem.MetricsCounters.ConcurrentRequests.Mark();
                fileSystem.MetricsCounters.RequestsPerSecondCounter.Mark();
            }
	    }

	    public void Init()
		{
			if (initialized)
				return;
			initialized = true;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
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

        public async Task<HttpResponseMessage> HandleActualRequest(RavenBaseApiController controller,
		                                                           Func<Task<HttpResponseMessage>> action,
		                                                           Func<HttpException, HttpResponseMessage> onHttpException)
	    {
	        HttpResponseMessage response = null;
            cancellationToken.ThrowIfCancellationRequested();

	        Stopwatch sw = Stopwatch.StartNew();
	        try
	        {
	            Interlocked.Increment(ref concurrentRequests);
	            if (IsWriteRequest(controller.InnerRequest))
	            {
	                lastWriteRequest = SystemTime.UtcNow;
	            }

                if (controller.SetupRequestToProperDatabase(this))
	            {
	                response = await action();
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

	    // Cross-Origin Resource Sharing (CORS) is documented here: http://www.w3.org/TR/cors/
        public void AddAccessControlHeaders(RavenBaseApiController controller, HttpResponseMessage msg)
		{
			if (string.IsNullOrEmpty(landlord.SystemConfiguration.AccessControlAllowOrigin))
				return;

			controller.AddHeader("Access-Control-Allow-Credentials", "true", msg);

			bool originAllowed = landlord.SystemConfiguration.AccessControlAllowOrigin == "*" ||
					landlord.SystemConfiguration.AccessControlAllowOrigin.Split(' ')
						.Any(o => o == controller.GetHeader("Origin"));
			if (originAllowed)
			{
				controller.AddHeader("Access-Control-Allow-Origin", controller.GetHeader("Origin"), msg);
			}

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

        public void SetThreadLocalState(HttpHeaders innerHeaders, string databaseName)
        {
            CurrentOperationContext.Headers.Value = new NameValueCollection();
            foreach (var innerHeader in innerHeaders)
                CurrentOperationContext.Headers.Value[innerHeader.Key] = innerHeader.Value.FirstOrDefault();

            CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = string.Empty;
            CurrentOperationContext.User.Value = null;

            LogContext.DatabaseName.Value = databaseName;
            var disposable = LogManager.OpenMappedContext("database", databaseName ?? Constants.SystemDatabase);

            CurrentOperationContext.RequestDisposables.Value.Add(disposable);
        }

		public void ResetThreadLocalState()
		{
			try
			{
				CurrentOperationContext.Headers.Value = new NameValueCollection();
				CurrentOperationContext.User.Value = null;
				LogContext.DatabaseName.Value = null;
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


		private static bool IsWriteRequest(HttpRequestMessage request)
		{
			return AbstractRequestAuthorizer.IsGetRequest(request.Method.Method, request.RequestUri.AbsoluteUri) == false;
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

		public void DecrementRequestCount()
		{
			Interlocked.Decrement(ref physicalRequestsCount);
		}

        private void FinalizeRequestProcessing(RavenBaseApiController controller, HttpResponseMessage response, Stopwatch sw)
		{
			LogHttpRequestStatsParams logHttpRequestStatsParam = null;
		    try
		    {
		        StringBuilder sb = null;
		        if (controller.CustomRequestTraceInfo != null)
		        {
		            sb = new StringBuilder();
                    foreach (var action in controller.CustomRequestTraceInfo)
                    {
                        action(sb);
                        sb.AppendLine();
                    }
		            while (sb.Length > 0)
		            {
		                if (!char.IsWhiteSpace(sb[sb.Length - 1])) 
                            break;
		                sb.Length--;
		            }
		        }
		        logHttpRequestStatsParam = new LogHttpRequestStatsParams(
		            sw,
		            GetHeaders(controller.InnerHeaders), 
		            controller.InnerRequest.Method.Method,
		            response != null ? (int) response.StatusCode : 500,
		            controller.InnerRequest.RequestUri.ToString(),
		            sb != null ? sb.ToString() : null
		            );
		    }
		    catch (Exception e)
		    {
		        Logger.WarnException("Could not gather information to log request stats", e);
		    }

		    if (logHttpRequestStatsParam == null || sw == null)
				return;

			sw.Stop();

		    controller.MarkRequestDuration(sw.ElapsedMilliseconds);

			LogHttpRequestStats(controller,logHttpRequestStatsParam, controller.TenantName);

			TraceRequest(logHttpRequestStatsParam, controller.TenantName);

		}

		private void TraceRequest(LogHttpRequestStatsParams requestLog, string databaseName)
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

		private NameValueCollection GetHeaders(HttpHeaders innerHeaders)
		{
			var result = new NameValueCollection();
			foreach (var innerHeader in innerHeaders)
			{
				result.Add(innerHeader.Key, innerHeader.Value.FirstOrDefault());
			}

			return result;
		}

		private void LogHttpRequestStats(RavenBaseApiController controller, LogHttpRequestStatsParams logHttpRequestStatsParams, string databaseName)
		{
			if (Logger.IsDebugEnabled == false)
				return;

			if (controller is StudioController || controller is HardRouteController || controller is SilverlightController)
				return;

			// we filter out requests for the UI because they fill the log with information
			// we probably don't care about them anyway. That said, we do output them if they take too
			// long.
			if (logHttpRequestStatsParams.Headers["Raven-Timer-Request"] == "true" &&
				logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds <= 25)
				return;

			var curReq = Interlocked.Increment(ref reqNum);
			Logger.Debug("Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {5,-10} - {3} - {4}",
							   curReq,
							   logHttpRequestStatsParams.HttpMethod,
							   logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds,
							   logHttpRequestStatsParams.ResponseStatusCode,
							   logHttpRequestStatsParams.RequestUri,
							   databaseName);
		    if (string.IsNullOrWhiteSpace(logHttpRequestStatsParams.CustomInfo) == false)
		        Logger.Debug(logHttpRequestStatsParams.CustomInfo);
		}


		private void IdleOperations(object state)
		{
			if ((SystemTime.UtcNow - lastWriteRequest).TotalMinutes < 1)
				return;// not idle, we just had a write request coming in

			try
			{
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
					documentDatabase.Value.Result.RunIdleOperations();
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
				landlord.Cleanup(db, skipIfActive: true);
			}
		}
	}
}