using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
	public class SearchController : RavenFsApiController
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

                return this.GetMessageWithObject(result);
            }
        }

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/search")]
        public HttpResponseMessage Get(string query, [FromUri] string[] sort)
		{
			int results;
			var keys = Search.Query(query, sort, Paging.Start, Paging.PageSize, out results);

            var list = new List<FileHeader>();

			Storage.Batch(accessor => list.AddRange(keys.Select(accessor.ReadFile).Where(x => x != null)));

			var result = new SearchResults
			{
				Start = Paging.Start,
				PageSize = Paging.PageSize,
				Files = list,
				FileCount = results
			};

            return this.GetMessageWithObject(result);
		}

		[HttpDelete]
		[RavenRoute("fs/{fileSystemName}/search")]
		public HttpResponseMessage DeleteByQuery(string query)
		{
			var status = new DeleteByQueryOperationStatus();
			var cts = new CancellationTokenSource();

			var task = Task.Factory.StartNew(() =>
			{
				try
				{
					int totalResults;
					status.LastProgress = "Searching for files matching the query...";
					var keys = Search.Query(query, null, 0, int.MaxValue, out totalResults);
					status.LastProgress = string.Format("Deleting {0} files...", totalResults);
					Action<string> progress = s => status.LastProgress = s;

					DeleteFiles(keys, totalResults, progress);

					FileSystem.Synchronizations.StartSynchronizeDestinationsInBackground();
				}
				catch (Exception e)
				{
					status.Faulted = true;
					status.State = RavenJObject.FromObject(new
					{
						Error = e.ToString()
					});
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
					status.Completed = true;
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
			});
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
					catch (Exception e)
					{
						//ignore files that are being synced
						continue;
					}

					var metadata = file.Metadata;
					if (metadata == null || metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
						continue;

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

		private class DeleteByQueryOperationStatus : IOperationState
		{
			public bool Completed { get; set; }
			public string LastProgress { get; set; }
			public string ExceptionDetails { get; set; }
			public bool Faulted { get; set; }
			public RavenJToken State { get; set; }
		}
	}
}
