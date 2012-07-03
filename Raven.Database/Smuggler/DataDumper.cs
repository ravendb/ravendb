// -----------------------------------------------------------------------
//  <copyright file="DataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
	public class DataDumper : SmugglerApiBase
	{
		public DataDumper(DocumentDatabase database, SmugglerOptions options) : base(options)
		{
			_database = database;
		}

		private readonly DocumentDatabase _database;

		protected override void EnsureDatabaseExists()
		{
			ensuredDatabaseExists = true;
		}

		protected override Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
		{
			var totalCount = 0;
			while (true)
			{
				var array = GetAttachments(totalCount, lastEtag);
				if (array.Length == 0)
				{
					ShowProgress("Done with reading attachments, total: {0}", totalCount);
					return lastEtag;
				}
				totalCount += array.Length;
				ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", array.Length, totalCount);
				foreach (var item in array)
				{
					item.WriteTo(jsonWriter);
				}
				lastEtag = new Guid(array.Last().Value<string>("Etag"));
			}
		}

		protected override void FlushBatch(List<RavenJObject> batch)
		{
			var sw = Stopwatch.StartNew();

			_database.Batch(batch.Select(x =>
			{
				var metadata = x.Value<RavenJObject>("@metadata");
				var key = metadata.Value<string>("@id");
				x.Remove("@metadata");
				return new PutCommandData
				{
					Document = x,
					Etag = null,
					Key = key,
					Metadata = metadata
				};
			}).ToArray());

			ShowProgress("Wrote {0:#,#} documents in {1:#,#;;0} ms", batch.Count, sw.ElapsedMilliseconds);
			batch.Clear();
		}

		protected override RavenJArray GetDocuments(Guid lastEtag)
		{
			const int dummy = 0;
			return _database.GetDocuments(dummy, 128, lastEtag);
		}

		protected override RavenJArray GetIndexes(int totalCount)
		{
			return _database.GetIndexes(totalCount, 128);
		}

		protected override void PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			// we filter out content length, because getting it wrong will cause errors 
			// in the server side when serving the wrong value for this header.
			// worse, if we are using http compression, this value is known to be wrong
			// instead, we rely on the actual size of the data provided for us
			attachmentExportInfo.Metadata.Remove("Content-Length");
			_database.PutStatic(attachmentExportInfo.Key, null, new MemoryStream(attachmentExportInfo.Data), attachmentExportInfo.Metadata);
		}

		protected override void PutIndex(string indexName, RavenJToken index)
		{
			_database.PutIndex(indexName, index.Value<RavenJObject>("definition").JsonDeserialization<IndexDefinition>());
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			if (Progress != null)
			{
				Progress(string.Format(format, args));
			}
		}

		private RavenJArray GetAttachments(int start, Guid? etag)
		{
			var array = new RavenJArray();
			var attachmentInfos = _database.GetAttachments(start, 128, etag, null);

			foreach (var attachmentInfo in attachmentInfos)
			{
				var attachment = _database.GetStatic(attachmentInfo.Key);
				if (attachment == null)
					return null;
				var data = attachment.Data;
				attachment.Data = () =>
				{
					var memoryStream = new MemoryStream();
					_database.TransactionalStorage.Batch(accessor => data().CopyTo(memoryStream));
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