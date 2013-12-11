using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Search;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Conflictuality;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Util.Streams;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public abstract class RavenFsApiController : ApiController
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private PagingInfo paging;
		private NameValueCollection queryString;

		private RavenFileSystem ravenFileSystem;

		public RavenFileSystem RavenFileSystem
		{
			get 
			{
				return ravenFileSystem ??
				       (ravenFileSystem = (RavenFileSystem) ControllerContext.Configuration.DependencyResolver.GetService(typeof (RavenFileSystem)));
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

		private NameValueCollection QueryString
		{
			get { return queryString ?? (queryString = HttpUtility.ParseQueryString(Request.RequestUri.Query)); }
		}

		protected TransactionalStorage Storage
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
							                   ContentLength = length,
							                   ContentRange = contentRange,
						                   }
				                   };

			return response;
		}

		protected void AssertFileIsNotBeingSynced(string fileName, StorageActionsAccessor accessor,
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
					throw new HttpResponseException(Request.CreateResponse(HttpStatusCode.PreconditionFailed, beingSyncedException));
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
	}
}