using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Common;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public abstract class BaseFileSystemApiController : ResourceApiController<RavenFileSystem, FileSystemsLandlord>
    {
        private PagingInfo paging;
        private NameValueCollection queryString;

        public string FileSystemName
        {
            get
            {
                return ResourceName;
            }
        }

        public RavenFileSystem FileSystem
        {
            get
            {
                return Resource;
            }
        }

        public override ResourceType ResourceType
        {
            get
            {
                return ResourceType.FileSystem;
            }
        }

        public override void MarkRequestDuration(long duration)
        {
            if (Resource == null)
                return;
            Resource.MetricsCounters.RequestDurationMetric.Update(duration);
            Resource.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
        }

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

        protected NameValueCollection QueryString
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

        protected class PagingInfo
        {
            public int PageSize;
            public int Start;
        }

        protected Raven.Abstractions.FileSystem.FileSystemInfo GetSourceFileSystemInfo()
        {
            var json = GetHeader(SyncingMultipartConstants.SourceFileSystemInfo);

            return RavenJObject.Parse(json).JsonDeserialization<Raven.Abstractions.FileSystem.FileSystemInfo>();
        }

        protected virtual RavenJObject GetFilteredMetadataFromHeaders(IEnumerable<KeyValuePair<string, IEnumerable<string>>> headers)
        {
            return headers.FilterHeadersToObject();
        }
    }
}
