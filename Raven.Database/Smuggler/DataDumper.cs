// -----------------------------------------------------------------------
//  <copyright file="DataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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

        protected async override Task EnsureDatabaseExists()
		{
			EnsuredDatabaseExists = true;
		}

		protected override async Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag)
		{
			var totalCount = 0;
			while (true)
			{
				var array = GetAttachments(totalCount, lastEtag);
				if (array.Length == 0)
				{
					var databaseStatistics = await GetStats();
					if (lastEtag == null) lastEtag = Etag.Empty;
					if (lastEtag.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
					{
						lastEtag = EtagUtil.Increment(lastEtag, SmugglerOptions.BatchSize);
						ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
						continue;
					}
					ShowProgress("Done with reading attachments, total: {0}", totalCount);
					return lastEtag;
				}
				totalCount += array.Length;
				ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length, totalCount);
				foreach (var item in array)
				{
					item.WriteTo(jsonWriter);
				}
				lastEtag = Etag.Parse(array.Last().Value<string>("Etag"));
			}
		}

		protected override Task<RavenJArray> GetTransformers(int start)
		{
			return new CompletedTask<RavenJArray>(database.GetTransformers(start, SmugglerOptions.BatchSize));
		}

		protected async override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag)
		{
			const int dummy = 0;

			var enumerator = database.GetDocuments(dummy, SmugglerOptions.BatchSize, lastEtag)
				.ToList()
				.Cast<RavenJObject>()
				.GetEnumerator();

			return new AsyncEnumeratorBridge<RavenJObject>(enumerator);
		}

		protected override Task<RavenJArray> GetIndexes(int totalCount)
		{
			return new CompletedTask<RavenJArray>(database.GetIndexes(totalCount, 128));
		}

		protected override Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			if (attachmentExportInfo != null)
			{
				// we filter out content length, because getting it wrong will cause errors 
				// in the server side when serving the wrong value for this header.
				// worse, if we are using http compression, this value is known to be wrong
				// instead, we rely on the actual size of the data provided for us
				attachmentExportInfo.Metadata.Remove("Content-Length");
				database.PutStatic(attachmentExportInfo.Key, null, attachmentExportInfo.Data,
									attachmentExportInfo.Metadata);
			}

			return new CompletedTask();
		}

		protected override Task PutDocument(RavenJObject document)
		{
			if (document != null)
			{
				var metadata = document.Value<RavenJObject>("@metadata");
				var key = metadata.Value<string>("@id");
				document.Remove("@metadata");

				database.Put(key, null, document, metadata, null);
			}

			return new CompletedTask();
		}

		protected override Task PutTransformer(string transformerName, RavenJToken transformer)
		{
			if (transformer != null)
			{
				var transformerDefinition =
					JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());
				database.PutTransform(transformerName, transformerDefinition);
			}

			return new CompletedTask();
		}

		protected override Task<string> GetVersion()
		{
			return new CompletedTask<string>(DocumentDatabase.ProductVersion);
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			if (index != null)
			{
				database.PutIndex(indexName, index.Value<RavenJObject>("definition").JsonDeserialization<IndexDefinition>());
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

		private RavenJArray GetAttachments(int start, Etag etag)
		{
			var array = new RavenJArray();
			var attachmentInfos = database.GetAttachments(start, 128, etag, null, 1024 * 1024 * 10);

			foreach (var attachmentInfo in attachmentInfos)
			{
				var attachment = database.GetStatic(attachmentInfo.Key);
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