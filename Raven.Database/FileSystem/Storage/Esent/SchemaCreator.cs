using System;
using System.Text;

using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.FileSystem.Storage.Esent
{
	public class SchemaCreator
	{
		public const string SchemaVersion = "0.7";
		private readonly Session session;

		public SchemaCreator(Session session)
		{
			this.session = session;
		}

		public void Create(string database)
		{
			JET_DBID dbid;
			Api.JetCreateDatabase(session, database, null, out dbid, CreateDatabaseGrbit.None);
			try
			{
				using (var tx = new Transaction(session))
				{
					CreateDetailsTable(dbid);
					CreateFilesTable(dbid);
					CreateConfigTable(dbid);
					CreateUsageTable(dbid);
					CreatePagesTable(dbid);
					CreateSignaturesTable(dbid);
					tx.Commit(CommitTransactionGrbit.None);
				}
			}
			finally
			{
				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
			}
		}


		private void CreateDetailsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "details", 1, 80, out tableid);
			JET_COLUMNID id;
			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
			}, null, 0, out id);

			JET_COLUMNID schemaVersion;
			Api.JetAddColumn(session, tableid, "schema_version", new JET_COLUMNDEF
			{
				cbMax = Encoding.Unicode.GetByteCount(SchemaVersion),
				cp = JET_CP.Unicode,
				coltyp = JET_coltyp.Text,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
			}, null, 0, out schemaVersion);

			JET_COLUMNID fileCount;
			var bytes = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "file_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate
			}, bytes, bytes.Length, out fileCount);


			using (var update = new Update(session, tableid, JET_prep.Insert))
			{
				Api.SetColumn(session, tableid, id, Guid.NewGuid().ToByteArray());
				Api.SetColumn(session, tableid, schemaVersion, SchemaVersion, Encoding.Unicode);
				Api.SetColumn(session, tableid, fileCount, 0);
				update.Save();
			}
		}

		private void CreatePagesTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "pages", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "page_strong_hash", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 20,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "page_weak_hash", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				cbMax = 4 * StorageConstants.MaxPageSize, // handle possible data expansion because of codecs usage
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			var one = BitConverter.GetBytes(1);
			Api.JetAddColumn(session, tableid, "usage_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
			}, one, one.Length, out columnid);

			var indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);

			indexDef = "+page_weak_hash\0+page_strong_hash\0\0";
			Api.JetCreateIndex(session, tableid, "by_keys", CreateIndexGrbit.IndexUnique, indexDef, indexDef.Length,
							   80);
		}

		private void CreateSignaturesTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "signatures", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);


			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 1024,
				coltyp = JET_coltyp.LongText,
				grbit = ColumndefGrbit.ColumnNotNULL,
				cp = JET_CP.Unicode
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "created_at", new JET_COLUMNDEF
			{
				cbMax = 1024,
				coltyp = JET_coltyp.DateTime,
				grbit = ColumndefGrbit.ColumnNotNULL,
				cp = JET_CP.Unicode
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "level", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnNotNULL,
				cp = JET_CP.Unicode
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				cbMax = 1024 * 1024 * 1024,
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			string indexDef = "+id\0+level\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);

			indexDef = "+name\0\0";
			Api.JetCreateIndex(session, tableid, "by_name", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
							   80);
		}
		private void CreateUsageTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "usage", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 1024,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "file_pos", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "page_id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "page_size", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			string indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);

			indexDef = "+name\0+file_pos\0\0";

			Api.JetCreateIndex2(session, tableid, new[]
			{
				new JET_INDEXCREATE
				{
					szIndexName = "by_name_and_pos",
					cbKey = indexDef.Length,
					cbKeyMost = SystemParameters.KeyMost,
					cbVarSegMac = SystemParameters.KeyMost,
					szKey = indexDef,
					grbit = CreateIndexGrbit.None,
					ulDensity = 80
				}
			}, 1);
		}
		private void CreateFilesTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "files", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 1024,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
			{
				cbMax = 1024 * 512,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "total_size", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 8,
				grbit = ColumndefGrbit.ColumnFixed
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "uploaded_size", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 8,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);


			string indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);

			indexDef = "+name\0\0";
			Api.JetCreateIndex2(session, tableid, new []{new JET_INDEXCREATE()
			{
				szIndexName = "by_name",
				grbit = CreateIndexGrbit.IndexUnique,
				cbKey = indexDef.Length,
				szKey = indexDef,
				cbKeyMost = SystemParameters.KeyMost,
                cbVarSegMac = SystemParameters.KeyMost,
				ulDensity = 80
			}},1);
			
			indexDef = "+etag\0\0";
			Api.JetCreateIndex(session, tableid, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
							   80);
		}

		private void CreateConfigTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "config", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 1024,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
			{
				cbMax = 1024 * 512,
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			string indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   80);

			indexDef = "+name\0\0";
			Api.JetCreateIndex(session, tableid, "by_name", CreateIndexGrbit.IndexUnique, indexDef, indexDef.Length,
							   80);
		}

		public static void UpdateVersion(Session session, JET_DBID dbid, string newVersion)
		{
			using (var table = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				if (Api.TryMoveFirst(session, table) == false)
					throw new InvalidOperationException("Could not find details row");
				using (var update = new Update(session, table, JET_prep.Replace))
				{
					var schemaVersion = Api.GetTableColumnid(session, table, "schema_version");
					Api.SetColumn(session, table, schemaVersion, newVersion, Encoding.Unicode);
					update.Save();
				}
			}
		}
	}
}
