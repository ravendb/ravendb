//-----------------------------------------------------------------------
// <copyright file="CreateSampleDb.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Database utilities.
    /// </summary>
    internal partial class Dbutil
    {
        /// <summary>
        /// Add a column to the table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to add the column to.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="coltyp">The type of the column.</param>
        private void AddColumn(JET_SESID sesid, JET_TABLEID tableid, string name, JET_coltyp coltyp)
        {
            JET_COLUMNID ignored;
            Api.JetAddColumn(
                sesid,
                tableid,
                name,
                new JET_COLUMNDEF() { coltyp = coltyp },
                null,
                0,
                out ignored);
        }

        /// <summary>
        /// Add a column to the table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to add the column to.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="coltyp">The type of the column.</param>
        /// <param name="cp">The codepage to use.</param>
        private void AddColumn(JET_SESID sesid, JET_TABLEID tableid, string name, JET_coltyp coltyp, JET_CP cp)
        {
            JET_COLUMNID ignored;
            Api.JetAddColumn(
                sesid,
                tableid,
                name,
                new JET_COLUMNDEF() { coltyp = coltyp, cp = cp },
                null,
                0,
                out ignored);
        }

        /// <summary>
        /// Create a sample database.
        /// </summary>
        /// <param name="args">Arguments for the command.</param>
        private void CreateSampleDb(string[] args)
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("specify the database", "args");
            }

            string database = args[0];

            using (var instance = new Instance("createsampledb"))
            {
                instance.Init();

                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, null, out dbid, CreateDatabaseGrbit.None);

                    // Creating a table opens it exclusively. That is when we can define the primary index.
                    JET_TABLEID tableid;
                    Api.JetCreateTable(session, dbid, "table", 16, 100, out tableid);
                    JET_COLUMNID columnidAutoinc;
                    Api.JetAddColumn(
                        session,
                        tableid,
                        "key",
                        new JET_COLUMNDEF() { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnAutoincrement },
                        null,
                        0,
                        out columnidAutoinc);
                    const string Indexdef1 = "+key\0\0";
                    Api.JetCreateIndex(session, tableid, "primary", CreateIndexGrbit.IndexPrimary, Indexdef1, Indexdef1.Length, 100);
                    Api.JetCloseTable(session, tableid);

                    using (var table = new Table(session, dbid, "table", OpenTableGrbit.None))
                    {
                        // Columns and secondary indexes can be added while the table is opened normally
                        this.AddColumn(session, table, "bit", JET_coltyp.Bit);
                        this.AddColumn(session, table, "byte", JET_coltyp.UnsignedByte);
                        this.AddColumn(session, table, "short", JET_coltyp.Short);
                        this.AddColumn(session, table, "long", JET_coltyp.Long);
                        this.AddColumn(session, table, "currency", JET_coltyp.Currency);
                        this.AddColumn(session, table, "single", JET_coltyp.IEEESingle);
                        this.AddColumn(session, table, "double", JET_coltyp.IEEEDouble);
                        this.AddColumn(session, table, "binary", JET_coltyp.LongBinary);
                        this.AddColumn(session, table, "ascii", JET_coltyp.LongText, JET_CP.ASCII);
                        this.AddColumn(session, table, "unicode", JET_coltyp.LongText, JET_CP.Unicode);

                        const string Indexdef2 = "+double\0-ascii\0\0";
                        Api.JetCreateIndex(session, table, "secondary", CreateIndexGrbit.None, Indexdef2, Indexdef2.Length, 100);

                        const string Indexdef3 = "-unicode\0+binary\0+key\0\0";
                        Api.JetCreateIndex(session, table, "unique_secondary", CreateIndexGrbit.IndexUnique, Indexdef3, Indexdef3.Length, 100);

                        using (var transaction = new Transaction(session))
                        {
                            IDictionary<string, JET_COLUMNID> columnids = Api.GetColumnDictionary(session, table);

                            // Null record
                            Api.JetPrepareUpdate(session, table, JET_prep.Insert);
                            Api.JetUpdate(session, table);

                            // Record that requires CSV quoting
                            using (var update = new Update(session, table, JET_prep.Insert))
                            {
                                Api.SetColumn(session, table, columnids["bit"], true);
                                Api.SetColumn(session, table, columnids["byte"], (byte)0x1e);
                                Api.SetColumn(session, table, columnids["short"], (short)0);
                                Api.SetColumn(session, table, columnids["long"], 1);
                                Api.SetColumn(session, table, columnids["currency"], Int64.MinValue);
                                Api.SetColumn(session, table, columnids["single"], (float)Math.E);
                                Api.SetColumn(session, table, columnids["double"], Math.PI);
                                Api.SetColumn(session, table, columnids["binary"], new byte[] { 0x1, 0x2, 0xea, 0x4f, 0x0 });
                                Api.SetColumn(session, table, columnids["ascii"], ",", Encoding.ASCII);
                                Api.SetColumn(session, table, columnids["unicode"], " \"quoting\" ", Encoding.Unicode);

                                update.Save();
                            }

                            for (int i = 0; i < 10; ++i)
                            {
                                using (var update = new Update(session, table, JET_prep.Insert))
                                {
                                    Api.SetColumn(session, table, columnids["unicode"], String.Format("Record {0}", i), Encoding.Unicode);
                                    Api.SetColumn(session, table, columnids["double"], i * 1.1);
                                    Api.SetColumn(session, table, columnids["long"], i);

                                    update.Save();
                                }
                            }

                            transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        }
                    }
                }
            }
        }
    }
}
