// -----------------------------------------------------------------------
//  <copyright file="DataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
	public class DataDumper : SmugglerApiBase
	{
		public DataDumper(DocumentDatabase database)
		{
			this.database = database;
		}

		private readonly DocumentDatabase database;

		protected async override Task EnsureDatabaseExists(RavenConnectionStringOptions to)
		{
			EnsuredDatabaseExists = true;
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonWriter"></param>
        /// <param name="options"></param>
        /// <param name="result"></param>
        /// <param name="maxEtags">Max etags are inclusive</param>
        protected async override void ExportDeletions(JsonTextWriter jsonWriter, SmugglerOptions options, ExportDataResult result, LastEtagsInfo maxEtags)
        {
            jsonWriter.WritePropertyName("DocsDeletions");
            jsonWriter.WriteStartArray();
            result.LastDocDeleteEtag = await ExportDocumentsDeletion(options, jsonWriter, result.LastDocDeleteEtag, maxEtags.LastDocDeleteEtag.IncrementBy(1));
            jsonWriter.WriteEndArray();

            jsonWriter.WritePropertyName("AttachmentsDeletions");
            jsonWriter.WriteStartArray();
            result.LastAttachmentsDeleteEtag = await ExportAttachmentsDeletion(options, jsonWriter, result.LastAttachmentsDeleteEtag, maxEtags.LastAttachmentsDeleteEtag.IncrementBy(1));
            jsonWriter.WriteEndArray();
        }


	    protected override void PurgeTombstones(ExportDataResult result)
	    {
	        database.TransactionalStorage.Batch(accessor =>
	        {
                // since remove all before is inclusive, but we want last etag for function FetchCurrentMaxEtags we modify ranges
	            accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicBackupsDocsTombstones, result.LastDocDeleteEtag.IncrementBy(-1));
                accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicBackupsAttachmentsTombstones, result.LastAttachmentsDeleteEtag.IncrementBy(-1));
	        });
	    }

	    public override LastEtagsInfo FetchCurrentMaxEtags()
	    {
	        LastEtagsInfo result = null;
            database.TransactionalStorage.Batch(accessor =>
            {
                result = new LastEtagsInfo
                         {
                             LastDocsEtag = accessor.Staleness.GetMostRecentDocumentEtag(),
                             LastAttachmentsEtag = accessor.Staleness.GetMostRecentAttachmentEtag()
                         };

                var lastDocumentTombstone = accessor.Lists.ReadLast(Constants.RavenPeriodicBackupsDocsTombstones);
                if (lastDocumentTombstone != null)
                    result.LastDocDeleteEtag = lastDocumentTombstone.Etag;

                var attachmentTombstones = 
                    accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, Etag.Empty, null, int.MaxValue)
                            .OrderBy(x => x.Etag).ToArray();
                if (attachmentTombstones.Any())
                {
                    result.LastAttachmentsDeleteEtag = attachmentTombstones.Last().Etag;
                }
            });

	        return result;
	    }

	    protected override async Task<Etag> ExportAttachments(RavenConnectionStringOptions src, JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
			var totalCount = 0;
	        var maxEtagReached = false;
		    while (true)
		    {
		        try
		        {
		            if (SmugglerOptions.Limit - totalCount <= 0 || maxEtagReached)
		            {
		                ShowProgress("Done with reading attachments, total: {0}", totalCount);
		                return lastEtag;
		            }
		            var maxRecords = Math.Min(SmugglerOptions.Limit - totalCount, SmugglerOptions.BatchSize);
		            var array = GetAttachments(totalCount, lastEtag, maxRecords);
		            if (array.Length == 0)
		            {
		                var databaseStatistics = await GetStats();
		                if (lastEtag == null) lastEtag = Etag.Empty;
		                if (lastEtag.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
		                {
		                    lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
		                    ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}",
		                                 lastEtag);
		                    continue;
		                }
		                ShowProgress("Done with reading attachments, total: {0}", totalCount);
		                return lastEtag;
		            }
		            totalCount += array.Length;
		            ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length, totalCount);
		            foreach (var item in array)
		            {
		                
		                var tempLastEtag = item.Value<string>("Etag");
                        if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                        {
                            maxEtagReached = true;
                            break;
                        }
                        item.WriteTo(jsonWriter);
		                lastEtag = tempLastEtag;
		            }
		        }
		        catch (Exception e)
		        {
                    ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    ShowProgress("Done with reading attachments, total: {0}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
		        }
		    }
		}

        protected Task<Etag> ExportDocumentsDeletion(SmugglerOptions options, JsonTextWriter jsonWriter, Etag startDocsEtag, Etag maxEtag)
        {
            var lastEtag = startDocsEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicBackupsDocsTombstones, startDocsEtag, maxEtag, int.MaxValue))
                {
                    var o = new RavenJObject
                    {
                        {"Key", listItem.Key}
                    };
                    o.WriteTo(jsonWriter);
                    lastEtag = listItem.Etag;
                }
            });
            return new CompletedTask<Etag>(lastEtag);

        }

		protected override Task<RavenJArray> GetTransformers(RavenConnectionStringOptions src, int start)
		{
			return new CompletedTask<RavenJArray>(database.Transformers.GetTransformers(start, SmugglerOptions.BatchSize));
		}

		protected async override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(RavenConnectionStringOptions src, Etag lastEtag, int limit)
		{
			const int dummy = 0;
			var enumerator = database.Documents.GetDocuments(dummy, Math.Min(SmugglerOptions.BatchSize, limit), lastEtag, CancellationToken.None)
				.ToList()
				.Cast<RavenJObject>()
				.GetEnumerator();

			return new AsyncEnumeratorBridge<RavenJObject>(enumerator);
		}

		protected override Task<RavenJArray> GetIndexes(RavenConnectionStringOptions src, int totalCount)
		{
			return new CompletedTask<RavenJArray>(database.Indexes.GetIndexes(totalCount, 128));
		}

		protected override Task PutAttachment(RavenConnectionStringOptions dst, AttachmentExportInfo attachmentExportInfo)
		{
			if (attachmentExportInfo != null)
			{
				// we filter out content length, because getting it wrong will cause errors 
				// in the server side when serving the wrong value for this header.
				// worse, if we are using http compression, this value is known to be wrong
				// instead, we rely on the actual size of the data provided for us
				attachmentExportInfo.Metadata.Remove("Content-Length");
				database.Attachments.PutStatic(attachmentExportInfo.Key, null, attachmentExportInfo.Data,
									attachmentExportInfo.Metadata);
			}

			return new CompletedTask();
		}

	    protected override Task DeleteAttachment(string key)
	    {
            database.Attachments.DeleteStatic(key, null);
            return new CompletedTask();
	    }

	    protected Task<Etag> ExportAttachmentsDeletion(SmugglerOptions options, JsonTextWriter jsonWriter, Etag startAttachmentsDeletionEtag, Etag maxAttachmentEtag)
	    {
            var lastEtag = startAttachmentsDeletionEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicBackupsAttachmentsTombstones, startAttachmentsDeletionEtag, maxAttachmentEtag, int.MaxValue))
                {
                    var o = new RavenJObject
                    {
                        {"Key", listItem.Key}
                    };
                    o.WriteTo(jsonWriter);
                    lastEtag = listItem.Etag;
                }
            });
            return new CompletedTask<Etag>(lastEtag);
	    }

	    protected override Task DeleteDocument(string key)
        {
            if (key != null)
            {
                database.Documents.Delete(key, null, null);
            }
            return new CompletedTask();
        }

		private List<JsonDocument> bulkInsertBatch = new List<JsonDocument>();

		protected override void PutDocument(RavenJObject document, SmugglerOptions options)
		{
			if (document != null)
			{
				var metadata = document.Value<RavenJObject>("@metadata");
				var key = metadata.Value<string>("@id");
				document.Remove("@metadata");

				bulkInsertBatch.Add(new JsonDocument
				{
					Key = key,
					Metadata = metadata,
					DataAsJson = document,
				});
				return;
			}

			var batchToSave = new List<IEnumerable<JsonDocument>> { bulkInsertBatch };
			bulkInsertBatch = new List<JsonDocument>();
			database.Documents.BulkInsert(new BulkInsertOptions { BatchSize = options.BatchSize, OverwriteExisting = true }, batchToSave, Guid.NewGuid());
		}

		protected override Task PutTransformer(string transformerName, RavenJToken transformer)
		{
			if (transformer != null)
			{
				var transformerDefinition =
					JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());
				database.Transformers.PutTransform(transformerName, transformerDefinition);
			}

			return new CompletedTask();
		}

		protected override Task<string> GetVersion(RavenConnectionStringOptions server)
		{
			return new CompletedTask<string>(DocumentDatabase.ProductVersion);
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			if (index != null)
			{
				database.Indexes.PutIndex(indexName, index.Value<RavenJObject>("definition").JsonDeserialization<IndexDefinition>());
			}

			return new CompletedTask();
		}

		protected override Task<DatabaseStatistics> GetStats()
		{
			return new CompletedTask<DatabaseStatistics>(database.Statistics);
		}

		protected override Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
		{
			return new CompletedTask<RavenJObject>(document);
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			if (Progress != null)
			{
				Progress(string.Format(format, args));
			}
		}

		private RavenJArray GetAttachments(int start, Etag etag, int maxRecords)
		{
			var array = new RavenJArray();
            var attachmentInfos = database.Attachments.GetAttachments(start, maxRecords, etag, null, 1024 * 1024 * 10);

			foreach (var attachmentInfo in attachmentInfos)
			{
                var attachment = database.Attachments.GetStatic(attachmentInfo.Key);
				if (attachment == null)
					return null;
				var data = attachment.Data;
				attachment.Data = () =>
				{
					var memoryStream = new MemoryStream();
					database.TransactionalStorage.Batch(accessor => data().CopyTo(memoryStream));
					memoryStream.Position = 0;
					return memoryStream;
				};

				var bytes = attachment.Data().ReadData();
				array.Add(
					new RavenJObject
					{
						{"Data", bytes},
						{"Metadata", attachmentInfo.Metadata},
						{"Key", attachmentInfo.Key},
						{"Etag", new RavenJValue(attachmentInfo.Etag.ToString())}
					});
			}
			return array;
		}

		public Action<string> Progress { get; set; }
	}
}