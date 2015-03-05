// -----------------------------------------------------------------------
//  <copyright file="From03To04.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Database.Config;
using Raven.Database.FileSystem.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
	public class From04To05 : IFileSystemSchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "0.4"; }
		}

		public void Init(InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			using (var table = new Table(session, dbid, "config", OpenTableGrbit.DenyRead | OpenTableGrbit.PermitDDL))
			{
				JET_COLUMNID newMetadataColumnId;

				Api.JetAddColumn(session, table, "metadata_new", new JET_COLUMNDEF
				{
					cbMax = 1024*512,
					coltyp = JET_coltyp.LongText,
					cp = JET_CP.Unicode,
					grbit = ColumndefGrbit.ColumnNotNULL
				}, null, 0, out newMetadataColumnId);
			}

			using (var table = new Table(session, dbid, "config", OpenTableGrbit.None))
			{
				Api.MoveBeforeFirst(session, table);

				var rows = 0;

				var columnDictionary = Api.GetColumnDictionary(session, table);

				var metadataColumn = columnDictionary["metadata"];
				var nameColumn = columnDictionary["name"];
				var newMetadataColumn = columnDictionary["metadata_new"];

				while (Api.TryMoveNext(session, table))
				{
					using (var insert = new Update(session, table, JET_prep.Replace))
					{
						var name = Api.RetrieveColumnAsString(session, table, nameColumn, Encoding.Unicode);
						var metadata = Api.RetrieveColumnAsString(session, table, metadataColumn, Encoding.Unicode);
						var fixedMetadata = GuidToEtagMigrationInConfigurations(metadata, name);

						Api.SetColumn(session, table, newMetadataColumn, fixedMetadata, Encoding.Unicode);

						insert.Save();
					}

					if (rows++ % 100 == 0)
					{
						output("Processed " + (rows) + " rows from metadata column in config table");
						Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
						Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
					}
				}

				Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
				
				// they cannot be run in transaction scope
				Api.JetDeleteColumn(session, table, "metadata");
				Api.JetRenameColumn(session, table, "metadata_new", "metadata", RenameColumnGrbit.None);
				
				Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
			}

			SchemaCreator.UpdateVersion(session, dbid, "0.5");
		}

		private static string GuidToEtagMigrationInConfigurations(string metadata, string name)
		{
			RavenJObject value = null;

			if (name.StartsWith(RavenFileNameHelper.SyncNamePrefix, StringComparison.InvariantCultureIgnoreCase))
			{
				value = RavenJObject.Parse(metadata);

				value["FileETag"] = Etag.Parse(Guid.Parse(value["FileETag"].ToString()).ToByteArray()).ToString();
			}
			else if (name.StartsWith(RavenFileNameHelper.SyncResultNamePrefix, StringComparison.InvariantCultureIgnoreCase))
			{
				value = RavenJObject.Parse(metadata);

				value["FileETag"] = Etag.Parse(Guid.Parse(value["FileETag"].ToString()).ToByteArray()).ToString();
			}
			else if (name.StartsWith(SynchronizationConstants.RavenSynchronizationSourcesBasePath, StringComparison.InvariantCultureIgnoreCase))
			{
				value = RavenJObject.Parse(metadata);

				value["LastSourceFileEtag"] = Etag.Parse(Guid.Parse(value["LastSourceFileEtag"].ToString()).ToByteArray()).ToString();
			}

			if (value == null)
				return metadata;

			var sb = new StringBuilder();
			using (var writer = new JsonTextWriter(new StringWriter(sb)))
				value.WriteTo(writer);

			return sb.ToString();
		}
	}
}