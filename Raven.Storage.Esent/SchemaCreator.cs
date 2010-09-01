using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;

namespace Raven.Storage.Esent
{
	[CLSCompliant(false)]
	public class SchemaCreator
	{
		public const string SchemaVersion = "2.71";
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
					CreateDocumentsTable(dbid);
                    CreateDocumentsBeingModifiedByTransactionsTable(dbid);
				    CreateTransactionsTable(dbid);
					CreateTasksTable(dbid);
					CreateMapResultsTable(dbid);
					CreateIndexingStatsTable(dbid);
					CreateFilesTable(dbid);
                    CreateQueueTable(dbid);
					CreateIdentityTable(dbid);

					tx.Commit(CommitTransactionGrbit.None);
				}
			}
			finally
			{
				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
			}
		}

		private void CreateIdentityTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "identity_table", 16, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);


			var defaultValue = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "val", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
			}, defaultValue, defaultValue.Length, out columnid);

			const string indexDef = "+key\0\0";
			Api.JetCreateIndex(session, tableid, "by_key", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   100);
		}

		private void CreateIndexingStatsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "indexes_stats", 16, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			var defaultValue = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "successes", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
			}, defaultValue, defaultValue.Length, out columnid);

			Api.JetAddColumn(session, tableid, "last_indexed_etag", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Binary,
				cbMax = 16,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);


			Api.JetAddColumn(session, tableid, "last_indexed_timestamp", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.DateTime,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "attempts", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
			}, defaultValue, defaultValue.Length, out columnid);

			Api.JetAddColumn(session, tableid, "errors", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
			}, defaultValue, defaultValue.Length, out columnid);

			const string indexDef = "+key\0\0";
			Api.JetCreateIndex(session, tableid, "by_key", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
			                   100);
		}

        private void CreateTransactionsTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "transactions", 256, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "tx_id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "timeout", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
            }, null, 0, out columnid);

            const string indexDef = "+tx_id\0\0";
            Api.JetCreateIndex(session, tableid, "by_tx_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);
        }

	    private void CreateDocumentsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "documents", 1024, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "last_modified", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.DateTime,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "locked_by_transaction", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnTagged,
            }, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);


			var indexDef = "+key\0\0";
			Api.JetCreateIndex(session, tableid, "by_key", CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique, indexDef, indexDef.Length,
			                   100);

			indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
			                   100);

            indexDef = "+etag\0\0";
            Api.JetCreateIndex(session, tableid, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);
		}

        private void CreateDocumentsBeingModifiedByTransactionsTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "documents_modified_by_transaction", 1024, 100, out tableid);
            JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);
			
			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Text,
                cp = JET_CP.Unicode,
                grbit = ColumndefGrbit.ColumnTagged
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
            {
                cbMax = 16,
				coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
            }, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "last_modified", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.DateTime,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "locked_by_transaction", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnFixed,
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.LongBinary,
                grbit = ColumndefGrbit.ColumnTagged
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
            {
				coltyp = JET_coltyp.LongBinary,
                grbit = ColumndefGrbit.ColumnTagged
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "delete_document", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Bit,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            var indexDef = "+key\0\0";
			Api.JetCreateIndex(session, tableid, "by_key", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);

			indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   100);

            indexDef = "+locked_by_transaction\0\0";
        	Api.JetCreateIndex2(session, tableid, new[]
        	{
        		new JET_INDEXCREATE
        		{
        			cbKey = indexDef.Length,
        			cbKeyMost = SystemParameters.KeyMost,
        			grbit = CreateIndexGrbit.IndexDisallowNull,
        			szIndexName = "by_tx",
        			szKey = indexDef,
        			ulDensity = 100,
        		},
        	}, 1);
        }

		private void CreateMapResultsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "mapped_results", 1024, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "view", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "document_key", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "reduce_key", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongText,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.None
			}, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "reduce_key_and_view_hashed", new JET_COLUMNDEF
            {
                cbMax = 32,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnNotNULL|ColumndefGrbit.ColumnFixed
			}, null, 0, out columnid);

			var indexDef = "+view\0+document_key\0+reduce_key\0\0";
			Api.JetCreateIndex(session, tableid, "by_pk", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
			                   100);

			indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   100);
			
			indexDef = "+view\0+document_key\0\0";
			Api.JetCreateIndex(session, tableid, "by_view_and_doc_key", CreateIndexGrbit.IndexDisallowNull, indexDef,
			                   indexDef.Length,
			                   100);

			indexDef = "+view\0\0";
			Api.JetCreateIndex(session, tableid, "by_view", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
			                   100);

            indexDef = "+reduce_key_and_view_hashed\0\0";
            Api.JetCreateIndex(session, tableid, "by_reduce_key_and_view_hashed", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);
		}

		private void CreateTasksTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "tasks", 256, 100, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "task", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongText,
				grbit = ColumndefGrbit.None
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "supports_merging", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Bit,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "task_type", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "for_index", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "added_at", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

			var indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
			                   100);
			indexDef = "+for_index\0\0";
			Api.JetCreateIndex(session, tableid, "by_index", CreateIndexGrbit.IndexIgnoreNull, indexDef, indexDef.Length,
			                   100);

			indexDef = "+supports_merging\0+for_index\0+task_type\0\0";
			Api.JetCreateIndex2(session, tableid, new[]{new JET_INDEXCREATE
			{
				cbKey = indexDef.Length,
				cbKeyMost = SystemParameters.KeyMost,
				grbit = CreateIndexGrbit.IndexIgnoreNull,
				szIndexName = "mergables_by_task_type",
				ulDensity = 100,
				szKey = indexDef
			}, },1);
		}

		private void CreateFilesTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "files", 1024, 100, out tableid);
			JET_COLUMNID columnid;


			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
			{
				cbMax = 255,
				coltyp = JET_coltyp.Text,
				cp = JET_CP.Unicode,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongBinary,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "etag", new JET_COLUMNDEF
			{
				cbMax = 16,
				coltyp = JET_coltyp.Binary,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL,
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "metadata", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.LongText,
				grbit = ColumndefGrbit.ColumnTagged
			}, null, 0, out columnid);

			var indexDef = "+name\0\0";
			Api.JetCreateIndex(session, tableid, "by_name", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
			                   100);

			indexDef = "+id\0\0";
			Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
							   100);


			indexDef = "+etag\0\0";
			Api.JetCreateIndex(session, tableid, "by_etag", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
							   100);
		}

	    public void CreateQueueTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "queue", 1024, 100, out tableid);
            JET_COLUMNID columnid;


            Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Text,
                cp = JET_CP.Unicode,
                grbit = ColumndefGrbit.ColumnTagged
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.LongBinary,
                grbit = ColumndefGrbit.ColumnTagged
            }, null, 0, out columnid);

            var bytes = BitConverter.GetBytes(0);
            Api.JetAddColumn(session, tableid, "read_count", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate
            }, bytes, bytes.Length, out columnid);


            var indexDef = "+name\0\0";
            Api.JetCreateIndex(session, tableid, "by_name", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);

            indexDef = "+id\0\0";
            Api.JetCreateIndex(session, tableid, "by_id", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);
        }
        
        private void CreateDetailsTable(JET_DBID dbid)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "details", 16, 100, out tableid);
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

			JET_COLUMNID documentCount;
			var bytes = BitConverter.GetBytes(0);
			Api.JetAddColumn(session, tableid, "document_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate
			}, bytes, bytes.Length, out documentCount);


			JET_COLUMNID attachmentCount;
			Api.JetAddColumn(session, tableid, "attachment_count", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate
			}, bytes, bytes.Length, out attachmentCount);

			using (var update = new Update(session, tableid, JET_prep.Insert))
			{
				Api.SetColumn(session, tableid, id, Guid.NewGuid().ToByteArray());
				Api.SetColumn(session, tableid, schemaVersion, SchemaVersion, Encoding.Unicode);
				Api.SetColumn(session, tableid, documentCount, 0);
				Api.SetColumn(session, tableid, attachmentCount, 0);
				update.Save();
			}
		}
	}
}