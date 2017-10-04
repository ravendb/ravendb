using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Lucene.Net.Index;
using Lucene.Net.Search;
using System.Net.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Util;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public class SearchController : BaseFileSystemApiController
    {
        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/search/Terms")]
        public HttpResponseMessage Terms([FromUri] string query  = "")
        {
            IndexSearcher searcher;
            using (Search.GetSearcher(out searcher))
            {
                string[] result = searcher.IndexReader.GetFieldNames(IndexReader.FieldOption.ALL)
                                    .Where(x => x.IndexOf(query, 0, StringComparison.InvariantCultureIgnoreCase) != -1).ToArray();

                return GetMessageWithObject(result);
            }
        }

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/search")]
        public HttpResponseMessage Get(string query, [FromUri] string[] sort)
        {
            int results;
            long durationInMs;
            var keys = Search.Query(query, sort, Paging.Start, Paging.PageSize, out results, out durationInMs);

            var list = new List<FileHeader>();

            Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

            var result = new SearchResults
            {
                Start = Paging.Start,
                PageSize = Paging.PageSize,
                Files = list,
                FileCount = results,
                DurationMilliseconds = durationInMs
            };

            return GetMessageWithObject(result);
        }

        [HttpDelete]
        [RavenRoute("fs/{fileSystemName}/search")]
        public HttpResponseMessage DeleteByQuery(string query)
        {
            var status = new DeleteByQueryOperationStatus();
            var cts = new CancellationTokenSource();

            int totalResults = 0;

            var task = Task.Factory.StartNew(() =>
            {
                try
                {
                    long durationInMs;
                    status.MarkProgress("Searching for files matching the query...");
                    var keys = Search.Query(query, null, 0, int.MaxValue, out totalResults, out durationInMs);
                    status.MarkProgress($"Deleting {totalResults} files...");
                    Action<string> progress = s => status.MarkProgress(s);

                    DeleteFiles(keys, totalResults, progress);

                    SynchronizationTask.Context.NotifyAboutWork();
                }
                catch (Exception e)
                {
                    status.MarkFaulted(e.ToString());
                    if (e is InvalidDataException)
                    {
                        status.ExceptionDetails = e.Message;
                    }
                    else
                    {
                        status.ExceptionDetails = e.ToString();
                    }

                    throw;
                }
                finally
                {
                    status.MarkCompleted(string.Format("Deleted {0} files", totalResults));
                }
            }, cts.Token);

            long id;
            FileSystem.Tasks.AddTask(task, status, new Actions.TaskActions.PendingTaskDescription 
            {
                StartTime = SystemTime.UtcNow,
                TaskType = Actions.TaskActions.PendingTaskType.DeleteFilesByQuery,
                Payload = string.Format("Delete by query: '{0}', for file system: {1}.", query, FileSystem.Name),
            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        private void DeleteFiles(IEnumerable<string> keys, int totalResults, Action<string> progress)
        {
            Storage.Batch(accessor =>
            {
                var files = keys.Select(accessor.ReadFile);
                foreach (var fileWithIndex in files.Select((value, i) => new {i, value}))
                {
                    var file = fileWithIndex.value;
                    var fileName = file.FullPath;
                    try
                    {
                        Synchronizations.AssertFileIsNotBeingSynced(fileName);
                    }
                    catch (Exception)
                    {
                        //ignore files that are being synced
                        continue;
                    }

                    var metadata = file.Metadata;
                    if (metadata == null || metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                        continue;
                    
                    Historian.Update(fileName, metadata);
                    Files.IndicateFileToDelete(fileName, null);

                    // don't create a tombstone for .downloading file
                    if (!fileName.EndsWith(RavenFileNameHelper.DownloadingFileSuffix))
                    {
                        Files.PutTombstone(fileName, metadata);
                        accessor.DeleteConfig(RavenFileNameHelper.ConflictConfigNameForFile(fileName)); // delete conflict item too
                    }

                    progress(string.Format("File {0}/{1} was deleted, name: '{2}'.", fileWithIndex.i, totalResults, fileName));
                }	
            });
        }

        private class DeleteByQueryOperationStatus : OperationStateBase
        {
            public string ExceptionDetails { get; set; }
        }
    }
}
