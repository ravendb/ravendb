using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Client.FileSystem;
using Raven.Database.Config;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.Server;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
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
using Raven.Abstractions.Exceptions;
using Raven.Client.FileSystem.Extensions;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Controllers
{
    public abstract class RavenFsApiController : RavenBaseApiController
    {
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        private PagingInfo paging;
        private NameValueCollection queryString;

        public override InMemoryRavenConfiguration SystemConfiguration
        {
            get { return this.FileSystemsLandlord.SystemConfiguration; }
        }

        public RavenFileSystem FileSystem
        {
            get
            {
                var fs = FileSystemsLandlord.GetFileSystemInternalAsync(FileSystemName);
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
                result = await RequestManager.HandleActualRequest(this, controllerContext, async () =>
                 {
                     RequestManager.SetThreadLocalState(ReadInnerHeaders, FileSystemName);
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

            if (IsInternalRequest == false)
                RequestManager.IncrementRequestCount();

            var fileSystemInternal = await FileSystemsLandlord.GetFileSystemInternalAsync(FileSystemName);
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

        public NotificationPublisher Publisher
        {
            get { return FileSystem.Publisher; }
        }

        public BufferPool BufferPool
        {
            get { return FileSystem.BufferPool; }
        }

        public SigGenerator SigGenerator
        {
            get { return FileSystem.SigGenerator; }
        }

        public Historian Historian
        {
            get { return FileSystem.Historian; }
        }

        private NameValueCollection QueryString
        {
            get { return queryString ?? (queryString = HttpUtility.ParseQueryString(Request.RequestUri.Query)); }
        }

        protected ITransactionalStorage Storage
        {
            get { return FileSystem.Storage; }
        }

        protected IndexStorage Search
        {
            get { return FileSystem.Search; }
        }

        protected FileActions Files
        {
            get { return FileSystem.Files; }
        }

        protected SynchronizationActions Synchronizations
        {
            get { return FileSystem.Synchronizations; }
        }

        protected FileLockManager FileLockManager
        {
            get { return FileSystem.FileLockManager; }
        }

        protected ConflictArtifactManager ConflictArtifactManager
        {
            get { return FileSystem.ConflictArtifactManager; }
        }

        protected ConflictDetector ConflictDetector
        {
            get { return FileSystem.ConflictDetector; }
        }

        protected ConflictResolver ConflictResolver
        {
            get { return FileSystem.ConflictResolver; }
        }

        protected SynchronizationTask SynchronizationTask
        {
            get { return FileSystem.SynchronizationTask; }
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

        protected HttpResponseException BadRequestException(string message)
        {
            return
                new HttpResponseException(new HttpResponseMessage(HttpStatusCode.BadRequest) { Content = new MultiGetSafeStringContent(message) });
        }

        protected class PagingInfo
        {
            public int PageSize;
            public int Start;
        }

        public override InMemoryRavenConfiguration ResourceConfiguration
        {
            get { return FileSystem.Configuration; }
        }

        public override bool TrySetupRequestToProperResource(out RequestWebApiEventArgs args)
        {
            if (!RavenFileSystem.IsRemoteDifferentialCompressionInstalled)
                throw new HttpException(503, "File Systems functionality is not supported. Remote Differential Compression is not installed.");

            var tenantId = FileSystemName;

            if (string.IsNullOrWhiteSpace(tenantId))
            {
                throw new HttpException(503, "Could not find a file system with no name");
            }

            var landlord = this.FileSystemsLandlord;

            Task<RavenFileSystem> resourceStoreTask;
            bool hasDb;
            try
            {
                hasDb = landlord.TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
            }
            catch (Exception e)
            {
                var cle = e as ConcurrentLoadTimeoutException;
                string msg;
                if (cle != null)
                {
                    msg = string.Format("The filesystem {0} is currently being loaded, but there are too many requests waiting for database load. Please try again later, database loading continues.", tenantId);
                }
                else
                {
                    var se = e.SimplifyException();
                    msg = "Could not open file system named: " + tenantId + ", " + se.Message;
                }

                Logger.WarnException(msg, e);
                throw new HttpException(503, msg, e);
            }
            if (hasDb)
            {
                try
                {
                    if (resourceStoreTask.Wait(TimeSpan.FromSeconds(30)) == false)
                    {
                        var msg = string.Format("The filesystem {0} is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, file system loading continues.", tenantId);
                        Logger.Warn(msg);
                        throw new TimeoutException(msg);
                    }

                    args = new RequestWebApiEventArgs()
                    {
                        Controller = this,
                        IgnoreRequest = false,
                        TenantId = tenantId,
                        FileSystem = resourceStoreTask.Result
                    };

                    if (args.IgnoreRequest)
                        return false;
                }
                catch (Exception e)
                {
                    var msg = "Could not open file system named: " + tenantId + Environment.NewLine + e;

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
            FileSystem.MetricsCounters.RequestDuationMetric.Update(duration);
        }

        protected FileSystemInfo GetSourceFileSystemInfo()
        {
            var json = GetHeader(SyncingMultipartConstants.SourceFileSystemInfo);

            return RavenJObject.Parse(json).JsonDeserialization<FileSystemInfo>();
        }

        #region Metadata Headers Handling


        private static readonly HashSet<string> HeadersToIgnoreClient = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // Raven internal headers
            Constants.RavenServerBuild,
            "Non-Authoritative-Information",
            "Raven-Timer-Request",

            //proxy
            "Reverse-Via",

            "Allow",
            "Content-Disposition",
            "Content-Encoding",
            "Content-Language",
            "Content-Location",
            "Content-MD5",
            "Content-Range",
            "Content-Type",
            "Expires",
            // ignoring this header, we handle this internally
            Constants.LastModified,
            // Ignoring this header, since it may
            // very well change due to things like encoding,
            // adding metadata, etc
            "Content-Length",
            // Special things to ignore
            "Keep-Alive",
            "X-Powered-By",
            "X-AspNet-Version",
            "X-Requested-With",
            "X-SourceFiles",
            // Request headers
            "Accept-Charset",
            "Accept-Encoding",
            "Accept",
            "Accept-Language",
            "Authorization",
            "Cookie",
            "Expect",
            "From",
            "Host",
            "If-Match",
            "If-Modified-Since",
            "If-None-Match",
            "If-Range",
            "If-Unmodified-Since",
            "Max-Forwards",
            "Referer",
            "TE",
            "User-Agent",
            //Response headers
            "Accept-Ranges",
            "Age",
            "Allow",
            Constants.MetadataEtagField,
            "Location",
            "Origin",
            "Retry-After",
            "Server",
            "Set-Cookie2",
            "Set-Cookie",
            "Vary",
            "Www-Authenticate",
            // General
            "Cache-Control",
            "Connection",
            "Date",
            "Pragma",
            "Trailer",
            "Transfer-Encoding",
            "Upgrade",
            "Via",
            "Warning",
            
            // Azure specific
            "X-LiveUpgrade",
            "DISGUISED-HOST",
            "X-SITE-DEPLOYMENT-ID",
        };

        protected static readonly IList<string> ReadOnlyHeaders = new List<string> { Constants.LastModified, Constants.MetadataEtagField }.AsReadOnly();

        protected virtual RavenJObject GetFilteredMetadataFromHeaders(IEnumerable<KeyValuePair<string, IEnumerable<string>>> headers)
        {
            return headers.FilterHeadersToObject();
        }

        #endregion Metadata Headers Handling
    }
}
