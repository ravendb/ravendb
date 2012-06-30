using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			JET_INSTANCE instance;
			Api.JetCreateInstance(out instance, "test");
			const string logsPath = ".";
			var x = new InstanceParameters(instance);
			x.CircularLog = true;
			x.Recovery = true;
			x.NoInformationEvent = false;
			x.CreatePathIfNotExist = true;
			x.EnableIndexChecking = true;
			x.TempDirectory = Path.Combine(logsPath, "temp");
			x.SystemDirectory = Path.Combine(logsPath, "system");
			x.LogFileDirectory = Path.Combine(logsPath, "logs");
			x.MaxVerPages = TranslateToSizeInDatabasePages(128, 1024 * 1024);
			x.BaseName = "RVN";
			x.EventSource = "Raven";
			x.LogBuffers = TranslateToSizeInDatabasePages(8192, 1024 * 1024);
			x.LogFileSize = ((64 / 4) * 1024);
			x.MaxSessions = 2048;
			x.MaxCursors = 2048;
			x.DbExtensionSize = TranslateToSizeInDatabasePages(8, 1024 * 1024);
			x.AlternateDatabaseRecoveryDirectory = logsPath;

			Api.JetInit(ref instance);

			using (var session = new Session(instance))
			{
				JET_DBID dbid;
				Api.JetCreateDatabase(session, "database", null, out dbid, CreateDatabaseGrbit.OverwriteExisting);
				Api.JetAttachDatabase(session, "database", AttachDatabaseGrbit.None);

				using (var tx = new Transaction(session))
				{
					CreateDocumentsTable(dbid, session);

					tx.Commit(CommitTransactionGrbit.None);
				}
				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				Api.JetDetachDatabase(session, "database");
			}

			Console.WriteLine("Starting to write");
			using (var session = new Session(instance))
			{
				var metadata = new byte[1024];
				var data = new byte[1024 * 14];

				var random = new Random();
				random.NextBytes(metadata);
				random.NextBytes(data);


				JET_DBID dbid;
				Api.JetAttachDatabase(session, "database", AttachDatabaseGrbit.None);
				Api.JetOpenDatabase(session, "database", null, out dbid, OpenDatabaseGrbit.None);

				var sp = Stopwatch.StartNew();
				IDictionary<string, JET_COLUMNID> docsColumns = null;
				for (int i = 0; i < 1 * 1000; i++)
				{
					using (var tx = new Transaction(session))
					{
						using (var table = new Table(session, dbid, "documents", OpenTableGrbit.None))
						{
							if (docsColumns == null)
								docsColumns = Api.GetColumnDictionary(session, table);
							for (int j = 0; j < 1024; j++)
							{
								using (var update = new Update(session, table, JET_prep.Insert))
								{
									Api.SetColumn(session, table, docsColumns["key"], "docs/" + i + "/" + j, Encoding.Unicode);
									Api.SetColumn(session, table, docsColumns["etag"], Guid.NewGuid().ToByteArray());
									Api.SetColumn(session, table, docsColumns["last_modified"], DateTime.Now);
									Api.SetColumn(session, table, docsColumns["locked_by_transaction"], false);
									Api.SetColumn(session, table, docsColumns["metadata"], metadata);
									using (var stream = new ColumnStream(session, table, docsColumns["data"]))
									using (var buffered = new BufferedStream(stream))
									{
										buffered.Write(data, 0, data.Length);
										buffered.Flush();
									}
									update.Save();
								}
							}
						}

						tx.Commit(CommitTransactionGrbit.LazyFlush);
					}
					Console.WriteLine(i);
				}

				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				Api.JetDetachDatabase(session, "database");

				using (var tx = new Transaction(session))
				{
					tx.Commit(CommitTransactionGrbit.WaitLastLevel0Commit);
				}
				sp.Stop();

				Console.WriteLine("Total: {0:#,#} ms, Per batch: {1:#,#.##}ms Per doc: {2:#,#.##} ms",
					sp.ElapsedMilliseconds,
					(double)sp.ElapsedMilliseconds / 1000,
					((double)sp.ElapsedMilliseconds / 1000) / 1024);

			}
		}


		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes, int multiply)
		{
			//This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
			double tempAmt = (double)sizeInMegabytes / SystemParameters.DatabasePageSize;
			int finalSize = (int)(tempAmt * multiply);
			return finalSize;
		}
		private static void CreateDocumentsTable(JET_DBID dbid, Session session)
		{
			JET_TABLEID tableid;
			Api.JetCreateTable(session, dbid, "documents", 1, 80, out tableid);
			JET_COLUMNID columnid;

			Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
			{
				coltyp = JET_coltyp.Long,
				grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnNotNULL
			}, null, 0, out columnid);

			Api.JetAddColumn(session, tableid, "key", new JET_COLUMNDEF
			{
				cbMax = 2048,
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

			CreateIndexes(tableid, session,
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_id",
							  szKey = "+id\0\0",
							  grbit = CreateIndexGrbit.IndexPrimary
						  },
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_etag",
							  szKey = "+etag\0\0",
							  grbit = CreateIndexGrbit.IndexDisallowNull
						  },
						  new JET_INDEXCREATE
						  {
							  szIndexName = "by_key",
							  szKey = "+key\0\0",
							  grbit = CreateIndexGrbit.IndexDisallowNull | CreateIndexGrbit.IndexUnique,
						  });
		}


		private static void CreateIndexes(JET_TABLEID tableid, Session session, params JET_INDEXCREATE[] indexes)
		{
			foreach (var index in indexes)
			{
				try
				{
					Api.JetCreateIndex(session, tableid, index.szIndexName, index.grbit, index.szKey, index.szKey.Length, 90);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not create index: " + index.szIndexName, e);
				}
			}
		}
	}
}