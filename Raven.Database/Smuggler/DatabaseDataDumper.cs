// -----------------------------------------------------------------------
//  <copyright file="DatabaseDataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Smuggler
{
	public class DatabaseDataDumper : SmugglerDatabaseApiBase
	{
        public DatabaseDataDumper(DocumentDatabase database, SmugglerDatabaseOptions options = null)
            : base(options ?? new SmugglerDatabaseOptions())
		{
			Operations = new SmugglerEmbeddedDatabaseOperations(database);
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

        public override Task Between(SmugglerBetweenOptions<RavenConnectionStringOptions> betweenOptions)
		{
			throw new NotSupportedException();
		}

		public Action<string> Progress
		{
			get
			{
				return ((SmugglerEmbeddedDatabaseOperations)Operations).Progress;
			}

			set
			{
				((SmugglerEmbeddedDatabaseOperations)Operations).Progress = value;
			}
		}
	}
}