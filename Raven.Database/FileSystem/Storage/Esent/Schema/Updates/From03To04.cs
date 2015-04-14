// -----------------------------------------------------------------------
//  <copyright file="From03To04.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema.Updates
{
	public class From03To04 : IFileSystemSchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "0.3"; }
		}

		public void Init(InMemoryRavenConfiguration configuration)
		{
		}

		public void Update(Session session, JET_DBID dbid, Action<string> output)
		{
			using (var table = new Table(session, dbid, "pages", OpenTableGrbit.DenyRead | OpenTableGrbit.PermitDDL))
			{
				JET_COLUMNID newDataColumnId;

				Api.JetAddColumn(session, table, "data_new", new JET_COLUMNDEF
				{
					cbMax = 4 * StorageConstants.MaxPageSize, // handle possible data expansion because of codecs usage
					coltyp = JET_coltyp.LongBinary,
					grbit = ColumndefGrbit.ColumnMaybeNull
				}, null, 0, out newDataColumnId);
			}
			
			using (var table = new Table(session, dbid, "pages", OpenTableGrbit.None))
			{
				Api.MoveBeforeFirst(session, table);

				var dataColumnId = Api.GetTableColumnid(session, table, "data");
				var newDataColumnId = Api.GetTableColumnid(session, table, "data_new");

				var rows = 0;

				while (Api.TryMoveNext(session, table))
				{
					using (var insert = new Update(session, table, JET_prep.Replace))
					{
						var value = Api.RetrieveColumn(session, table, dataColumnId);
						Api.SetColumn(session, table, newDataColumnId, value);

						insert.Save();
					}

					if (rows++ % 1000 == 0)
					{
						output("Processed " + (rows) + " rows from data column in pages table");
						Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush);
						Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
					}
				}

				Api.JetCommitTransaction(session, CommitTransactionGrbit.None);

				// they cannot be run in transaction scope
				Api.JetDeleteColumn(session, table, "data");
				Api.JetRenameColumn(session, table, "data_new", "data", RenameColumnGrbit.None);
				
				Api.JetBeginTransaction2(session, BeginTransactionGrbit.None);
			}

			SchemaCreator.UpdateVersion(session, dbid, "0.4");
		}
	}
}