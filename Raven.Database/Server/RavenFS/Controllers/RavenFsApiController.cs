using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;
using Raven.Abstractions;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Search;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Conflictuality;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public abstract class RavenFsApiController : RavenBaseApiController
	{
	    private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		private PagingInfo paging;
		private NameValueCollection queryString;

	    private FileSystemsLandlord landlord;
	    private RequestManager requestManager;

        public RequestManager RequestManager
        {
            get
            {
                if (Configuration == null)
                    return requestManager;
                return (RequestManager)Configuration.Properties[typeof(RequestManager)];
            }
        }
	    public RavenFileSystem RavenFileSystem
		{
			get
			{
			    var fs = FileSystemsLandlord.GetFileSystemInternal(FileSystemName);
                if (fs == null)
                {
                    throw new InvalidOperationException("Could not find a file system named: " + FileSystemName);
                }

                return fs.Result;
			}
		}

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			InnerInitialization(controllerContext);
            var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
            var result = new HttpResponseMessage();
            if (InnerRequest.Method.Method != "OPTIONS")
            {
                result = await RequestManager.HandleActualRequest(this, async () =>
                {
                    RequestManager.SetThreadLocalState(InnerHeaders, FileSystemName);
                    return await ExecuteActualRequest(controllerContext, cancellationToken, authorizer);
                }, httpException => GetMessageWithObject(new { Error = httpException.Message }, HttpStatusCode.ServiceUnavailable));
            }

            RequestManager.AddAccessControlHeaders(this, result);
            RequestManager.ResetThreadLocalState();

            return result;
		}


        private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken,
            MixedModeRequestAuthorizer authorizer)
        {
            HttpResponseMessage authMsg;
            if (authorizer.TryAuthorize(this, out authMsg) == false)
                return authMsg;

            var internalHeader = GetHeader("Raven-internal-request");
            if (internalHeader == null || internalHeader != "true")
                RequestManager.IncrementRequestCount();

            var fileSystemInternal = await FileSystemsLandlord.GetFileSystemInternal(FileSystemName);
            if (fileSystemInternal == null)
            {
                var msg = "Could not find a file system named: " + FileSystemName;
                return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
            }

            var sp = Stopwatch.StartNew();

            var result = await base.ExecuteAsync(controllerContext, cancellationToken);
            sp.Stop();
            AddRavenHeader(result, sp);

            return result;
        }


		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
            base.InnerInitialization(controllerContext);
            landlord = (FileSystemsLandlord)controllerContext.Configuration.Properties[typeof(FileSystemsLandlord)];
            requestManager = (RequestManager)controllerContext.Configuration.Properties[typeof(RequestManager)];

            var values = controllerContext.Request.GetRouteData().Values;
            if (values.ContainsKey("MS_SubRoutes"))
            {
                var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
                var selectedData = routeDatas.FirstOrDefault(data => data.Values.ContainsKey("fileSystemName"));

                if (selectedData != null)
                    FileSystemName = selectedData.Values["fileSystemName"] as string;
            }
            else
            {
                if (values.ContainsKey("fil"))
                    FileSystemName = values["fileSystemName"] as string;
            }
		    if (FileSystemName == null)
		        throw new InvalidOperationException("Could not find file system name for this request");
		}

	    public string FileSystemName { get; private set; }

	    public FileSystemsLandlord FileSystemsLandlord
        {
            get
            {
                if (Configuration == null)
                    return landlord;
                return (FileSystemsLandlord)Configuration.Properties[typeof(FileSystemsLandlord)];
            }
        }


		public NotificationPublisher Publisher
		{
			get { return RavenFileSystem.Publisher; }
		}

		public BufferPool BufferPool
		{
			get { return RavenFileSystem.BufferPool; }
		}

		public SigGenerator SigGenerator
		{
			get { return RavenFileSystem.SigGenerator; }
		}

		public Historian Historian
		{
			get { return RavenFileSystem.Historian; }
		}

	    public override InMemoryRavenConfiguration SystemConfiguration
	    {
	        get { return FileSystemsLandlord.SystemConfiguration; }
	    }

	    private NameValueCollection QueryString
		{
			get { return queryString ?? (queryString = HttpUtility.ParseQueryString(Request.RequestUri.Query)); }
		}

		protected ITransactionalStorage Storage
		{
			get { return RavenFileSystem.Storage; }
		}

		protected IndexStorage Search
		{
			get { return RavenFileSystem.Search; }
		}

		protected FileLockManager FileLockManager
		{
			get { return RavenFileSystem.FileLockManager; }
		}

		protected ConflictArtifactManager ConflictArtifactManager
		{
			get { return RavenFileSystem.ConflictArtifactManager; }
		}

		protected ConflictDetector ConflictDetector
		{
			get { return RavenFileSystem.ConflictDetector; }
		}

		protected ConflictResolver ConflictResolver
		{
			get { return RavenFileSystem.ConflictResolver; }
		}

		protected SynchronizationTask SynchronizationTask
		{
			get { return RavenFileSystem.SynchronizationTask; }
		}

		protected StorageOperationsTask StorageOperationsTask
		{
			get { return RavenFileSystem.StorageOperationsTask; }
		}

		protected PagingInfo Paging
		{
			get
			{
				if (paging != null)
					return paging;

				int start;
				int.TryParse(QueryString["start"], out start);

				int pageSize;
				if (int.TryParse(QueryString["pageSize"], out pageSize) == false)
					pageSize = 25;

				paging = new PagingInfo
					         {
						         PageSize = Math.Min(1024, Math.Max(1, pageSize)),
						         Start = Math.Max(start, 0)
					         };

				return paging;
			}
		}

		protected Task<T> Result<T>(T result)
		{
			var tcs = new TaskCompletionSource<T>();
			tcs.SetResult(result);
			return tcs.Task;
		}

		protected HttpResponseMessage StreamResult(string filename, Stream resultContent)
		{
			var response = new HttpResponseMessage
				               {
					               Headers =
						               {
							               TransferEncodingChunked = false
						               }
				               };
			long length;
			ContentRangeHeaderValue contentRange = null;
			if (Request.Headers.Range != null)
			{
				if (Request.Headers.Range.Ranges.Count != 1)
				{
					throw new InvalidOperationException("Can't handle multiple range values");
				}
				var range = Request.Headers.Range.Ranges.First();
				var from = range.From ?? 0;
				var to = range.To ?? resultContent.Length;

				length = (to - from);

				// "to" in Content-Range points on the last byte. In other words the set is: <from..to>  not <from..to)
				if (from < to)
				{
					contentRange = new ContentRangeHeaderValue(from, to - 1, resultContent.Length);
					resultContent = new LimitedStream(resultContent, from, to);
				}
				else
				{
					contentRange = new ContentRangeHeaderValue(0);
					resultContent = Stream.Null;
				}
			}
			else
			{
				length = resultContent.Length;
			}

			response.Content = new StreamContent(resultContent)
				                   {
					                   Headers =
						                   {
							                   ContentDisposition = new ContentDispositionHeaderValue("attachment")
								                                        {
									                                        FileName = filename
								                                        },
							                  // ContentLength = length,
							                   ContentRange = contentRange,
						                   }
				                   };

			return response;
		}

		protected void AssertFileIsNotBeingSynced(string fileName, IStorageActionsAccessor accessor,
		                                          bool wrapByResponseException = false)
		{
			if (FileLockManager.TimeoutExceeded(fileName, accessor))
			{
				FileLockManager.UnlockByDeletingSyncConfiguration(fileName, accessor);
			}
			else
			{
				Log.Debug("Cannot execute operation because file '{0}' is being synced", fileName);

				var beingSyncedException = new SynchronizationException(string.Format("File {0} is being synced", fileName));

				if (wrapByResponseException)
				{
					throw new HttpResponseException(Request.CreateResponse((HttpStatusCode)420, beingSyncedException));
				}

				throw beingSyncedException;
			}
		}

		protected HttpResponseException BadRequestException(string message)
		{
			return
				new HttpResponseException(new HttpResponseMessage(HttpStatusCode.BadRequest) {Content = new StringContent(message)});
		}

		protected HttpResponseException ConcurrencyResponseException(ConcurrencyException concurrencyException)
		{
			return new HttpResponseException(Request.CreateResponse(HttpStatusCode.MethodNotAllowed, concurrencyException));
		}

		protected class PagingInfo
		{
			public int PageSize;
			public int Start;
		}



        public override bool SetupRequestToProperDatabase(RequestManager rm)
        {
            var tenantId = FileSystemName;

            if (string.IsNullOrWhiteSpace(tenantId))
            {
                throw new HttpException(503, "Could not find a file system with no name");
            }

            Task<RavenFileSystem> resourceStoreTask;
            bool hasDb;
            try
            {
                hasDb = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
            }
            catch (Exception e)
            {
                var msg = "Could open file system named: " + tenantId;
                Logger.WarnException(msg, e);
                throw new HttpException(503, msg, e);
            }
            if (hasDb)
            {
                try
                {
                    if (resourceStoreTask.Wait(TimeSpan.FromSeconds(30)) == false)
                    {
                        var msg = "The filesystem " + tenantId +
                                  " is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, file system loading continues.";
                        Logger.Warn(msg);
                        throw new HttpException(503, msg);
                    }
                    var args = new BeforeRequestWebApiEventArgs()
                    {
                        Controller = this,
                        IgnoreRequest = false,
                        TenantId = tenantId,
                        FileSystem = resourceStoreTask.Result
                    };
                    rm.OnBeforeRequest(args);
                    if (args.IgnoreRequest)
                        return false;
                }
                catch (Exception e)
                {
                    var msg = "Could open file system named: " + tenantId;
                    Logger.WarnException(msg, e);
                    throw new HttpException(503, msg, e);
                }

                landlord.LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
            }
            else
            {
                var msg = "Could not find a file system named: " + tenantId;
                Logger.Warn(msg);
                throw new HttpException(503, msg);
            }
            return true;
        }

	    public override string TenantName
	    {
	        get { return "fs/" + FileSystemName; }
	    }

	    public override void MarkRequestDuration(long duration)
	    {
	        RavenFileSystem.MetricsCounters.RequestDuationMetric.Update(duration);
	    }
	}
}