// -----------------------------------------------------------------------
//  <copyright file="DataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Smuggler
{
	public class DataDumper : SmugglerApiBase
	{
		public DataDumper(DocumentDatabase database, SmugglerOptions options = null)
		{
			SmugglerOptions = options ?? new SmugglerOptions();
			Operations = new EmbeddedSmugglerOperations(database, SmugglerOptions);
		}

		public override async Task ExportDeletions(JsonTextWriter jsonWriter, ExportDataResult result, LastEtagsInfo maxEtagsToFetch)
		{
			jsonWriter.WritePropertyName("DocsDeletions");
			jsonWriter.WriteStartArray();
			result.LastDocDeleteEtag = await Operations.ExportDocumentsDeletion(jsonWriter, result.LastDocDeleteEtag, maxEtagsToFetch.LastDocDeleteEtag.IncrementBy(1));
			jsonWriter.WriteEndArray();

			jsonWriter.WritePropertyName("AttachmentsDeletions");
			jsonWriter.WriteStartArray();
			result.LastAttachmentsDeleteEtag = await Operations.ExportAttachmentsDeletion(jsonWriter, result.LastAttachmentsDeleteEtag, maxEtagsToFetch.LastAttachmentsDeleteEtag.IncrementBy(1));
			jsonWriter.WriteEndArray();
		}

		public override Task Between(SmugglerBetweenOptions betweenOptions)
		{
			throw new NotSupportedException();
		}

		public Action<string> Progress
		{
			get
			{
				return ((EmbeddedSmugglerOperations)Operations).Progress;
			}

			set
			{
				((EmbeddedSmugglerOperations)Operations).Progress = value;
			}
		}
	}
}