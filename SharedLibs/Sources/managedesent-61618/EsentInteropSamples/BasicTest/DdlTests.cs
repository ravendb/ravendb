//-----------------------------------------------------------------------
// <copyright file="DDLTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace BasicTest
{
    using System;
    using System.Diagnostics;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;

    internal class DdlTests
    {
        private readonly JET_coltyp[] coltyps = new[]
        {
            JET_coltyp.Bit,
            JET_coltyp.UnsignedByte,
            JET_coltyp.Short,
            JET_coltyp.Long,
            JET_coltyp.Currency,
            JET_coltyp.IEEESingle,
            JET_coltyp.IEEEDouble,
            JET_coltyp.DateTime,
            JET_coltyp.Binary,
            JET_coltyp.Text,
            JET_coltyp.LongBinary,
            JET_coltyp.LongText,
            VistaColtyp.UnsignedLong,
            VistaColtyp.LongLong,
            VistaColtyp.GUID,
            VistaColtyp.UnsignedShort
        };

        private readonly JET_DBID dbid;
        private readonly JET_SESID sesid;
        private readonly string table;

        public DdlTests(JET_SESID sesid, JET_DBID dbid, string table)
        {
            this.sesid = sesid;
            this.dbid = dbid;
            this.table = table;
        }

        public void Create()
        {
            Console.WriteLine("DDL creation");
            this.JetCreateTableColumnIndex();
            this.JetAddColumn();
            this.JetAddColumnDuplicate();
            this.JetCreateIndex();
            this.JetCreateIndexDuplicate();
            this.JetCreateTable();
            this.JetCreateTableDuplicate();
            this.JetDeleteColumn();
            this.JetDeleteIndex();
            this.JetDeleteTable();
            this.JetSetCurrentIndex();
        }

        public void Update()
        {
            Console.WriteLine("DDL update");

            using (var tableid = new Table(this.sesid, this.dbid, this.table, OpenTableGrbit.None))
            {
                Api.JetBeginTransaction(this.sesid);

                Console.WriteLine("\tDeleteIndex");
                Api.JetDeleteIndex(this.sesid, tableid, "index_columntodelete2");

                Console.WriteLine("\tDeleteColumn");
                Api.JetDeleteColumn(this.sesid, tableid, "columntodelete2");

                Console.WriteLine("\tCreateIndex");
                const string Key = "+recordID\0-autoinc\0+version\0-unicode\0\0";
                Api.JetCreateIndex(this.sesid, tableid, "newindex", CreateIndexGrbit.None, Key, Key.Length, 100);

                Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            }
        }

        private void JetAddColumn()
        {
            Console.WriteLine("\tJetAddColumn()");
            var columndef = new JET_COLUMNDEF();
            using (var tableid = new Table(this.sesid, this.dbid, this.table, OpenTableGrbit.None))
            {
                Api.JetBeginTransaction(this.sesid);
                foreach (JET_coltyp coltyp in this.coltyps)
                {
                    columndef.coltyp = coltyp;

                    JET_COLUMNID ignored;
                    Api.JetAddColumn(this.sesid, tableid, coltyp.ToString(), columndef, null, 0, out ignored);
                }

                Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            }
        }

        private void JetAddColumnDuplicate()
        {
            Console.WriteLine("\tJetAddColumnDuplicate()");
            var columndef = new JET_COLUMNDEF();
            using (var tableid = new Table(this.sesid, this.dbid, this.table, OpenTableGrbit.None))
            {
                Api.JetBeginTransaction(this.sesid);
                columndef.coltyp = JET_coltyp.Long;
                try
                {
                    JET_COLUMNID ignored;
                    Api.JetAddColumn(this.sesid, tableid, "recordID", columndef, null, 0, out ignored);
                    throw new Exception("Expected an EsentColumnDuplicateException");
                }
                catch (EsentErrorException)
                {
                    // expected
                }

                Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            }
        }

        private void JetCreateIndex()
        {
            Console.WriteLine("\tJetCreateIndex()");
            using (var tableid = new Table(this.sesid, this.dbid, this.table, OpenTableGrbit.None))
            {
                Api.JetBeginTransaction(this.sesid);
                foreach (JET_coltyp coltyp in this.coltyps)
                {
                    string name = String.Format("index_{0}", coltyp);
                    string definition = String.Format("+{0}\0\0", coltyp);
                    Api.JetCreateIndex(
                        this.sesid, tableid, name, CreateIndexGrbit.None, definition, definition.Length, 100);
                }

                Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            }
        }

        private void JetCreateIndexDuplicate()
        {
            Console.WriteLine("\tJetCreateIndexDuplicate()");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            Api.JetBeginTransaction(this.sesid);
            try
            {
                const string Description = "+recordID\0\0";
                Api.JetCreateIndex(
                    this.sesid, tableid, "secondary", CreateIndexGrbit.None, Description, Description.Length, 100);
                throw new Exception("Expected an EsentColumnDuplicateException");
            }
            catch (EsentIndexDuplicateException)
            {
                // expected
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        private void JetCreateTable()
        {
            const string TableName = "tabletodelete";
            Console.WriteLine("\tJetCreateTable()");
            Api.JetBeginTransaction(this.sesid);

            JET_TABLEID tableid;
            BasicClass.Assert(
                !Api.TryOpenTable(this.sesid, this.dbid, TableName, OpenTableGrbit.None, out tableid),
                "Able to open non-existent table");
            Api.JetCreateTable(this.sesid, this.dbid, TableName, 1, 100, out tableid);
            var columndef = new JET_COLUMNDEF();

            // Add a column and an index
            columndef.coltyp = JET_coltyp.LongBinary;
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, tableid, "column", columndef, null, 0, out columnid);
            Api.JetCreateIndex(this.sesid, tableid, "primary", CreateIndexGrbit.IndexPrimary, "+column\0\0", 9, 100);
            Api.JetCreateIndex(this.sesid, tableid, "secondary", CreateIndexGrbit.None, "-column\0\0", 9, 100);

            // Insert a record to force creation of the LV tree
            Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
            Api.JetSetColumn(
                this.sesid, tableid, columndef.columnid, new byte[2000], 2000, SetColumnGrbit.IntrinsicLV, null);
            Api.JetUpdate(this.sesid, tableid);

            Api.JetCloseTable(this.sesid, tableid);

            BasicClass.Assert(
                Api.TryOpenTable(this.sesid, this.dbid, TableName, OpenTableGrbit.None, out tableid),
                "Unable to open table");
            Api.JetCloseTable(this.sesid, tableid);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        private void JetCreateTableDuplicate()
        {
            Console.WriteLine("\tJetCreateTableDuplicate()");
            Api.JetBeginTransaction(this.sesid);
            try
            {
                JET_TABLEID tableid;
                Api.JetCreateTable(this.sesid, this.dbid, this.table, 1, 100, out tableid);
                throw new Exception("Expected an EsentTableDuplicateException");
            }
            catch (EsentTableDuplicateException)
            {
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        private void JetDeleteColumn()
        {
            Console.WriteLine("\tJetDeleteColumn()");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteColumn(this.sesid, tableid, "columntodelete");
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            try
            {
                Api.JetDeleteColumn(this.sesid, tableid, "columntodelete");
                throw new Exception("Expected an EsentColumnNotFoundException");
            }
            catch (EsentColumnNotFoundException)
            {
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        private void JetDeleteIndex()
        {
            Console.WriteLine("\tJetDeleteIndex()");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteIndex(this.sesid, tableid, "indextodelete");
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            try
            {
                Api.JetDeleteIndex(this.sesid, tableid, "indextodelete");
                throw new Exception("Expected an EsentIndexNotFoundException");
            }
            catch (EsentIndexNotFoundException)
            {
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        private void JetDeleteTable()
        {
            Console.WriteLine("\tJetDeleteTable()");
            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteTable(this.sesid, this.dbid, "tabletodelete");
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            try
            {
                Api.JetDeleteTable(this.sesid, this.dbid, "tabletodelete");
                throw new Exception("Expected an EsentObjectNotFoundException");
            }
            catch (EsentObjectNotFoundException)
            {
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        private void JetSetCurrentIndex()
        {
            Console.WriteLine("\tJetSetCurrentIndex()");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            foreach (JET_coltyp coltyp in this.coltyps)
            {
                string name = String.Format("index_{0}", coltyp);
                Api.JetSetCurrentIndex(this.sesid, tableid, name);
            }

            try
            {
                Api.JetSetCurrentIndex(this.sesid, tableid, "nonexistentindex");
                throw new Exception("Expected an EsentIndexNotFoundException");
            }
            catch (EsentIndexNotFoundException)
            {
            }

            Api.JetCloseTable(this.sesid, tableid);
        }

        private JET_INDEXCREATE MakeIndexcreate(string column)
        {
            var indexcreate = new JET_INDEXCREATE
            {
                szIndexName = String.Format("index_{0}", column),
                szKey = String.Format("-{0}\0\0", column),
            };
            indexcreate.cbKey = indexcreate.szKey.Length;
            return indexcreate;
        }

        private void JetCreateTableColumnIndex()
        {
            Console.WriteLine("\tJetCreateTableColumnIndex()");
            var columncreates = new JET_COLUMNCREATE[13];
            for (int i = 0; i < columncreates.Length; ++i)
            {
                columncreates[i] = new JET_COLUMNCREATE();
            }

            columncreates[0] = new JET_COLUMNCREATE { szColumnName = "recordID", coltyp = JET_coltyp.Long };

            columncreates[1].szColumnName = "tagged";
            columncreates[1].coltyp = VistaColtyp.LongLong;
            columncreates[1].grbit = ColumndefGrbit.ColumnTagged;

            columncreates[2].szColumnName = "separated_lv";
            columncreates[2].coltyp = JET_coltyp.LongBinary;

            columncreates[3].szColumnName = "compressed_unicode";
            columncreates[3].coltyp = JET_coltyp.LongText;
            columncreates[3].cp = JET_CP.Unicode;
            columncreates[3].grbit = Windows7Grbits.ColumnCompressed;

            columncreates[4].szColumnName = "compressed_ascii";
            columncreates[4].coltyp = JET_coltyp.LongText;
            columncreates[4].cp = JET_CP.ASCII;
            columncreates[4].grbit = Windows7Grbits.ColumnCompressed;

            columncreates[5].szColumnName = "compressed_binary";
            columncreates[5].coltyp = JET_coltyp.LongBinary;
            columncreates[5].grbit = Windows7Grbits.ColumnCompressed;

            columncreates[6].szColumnName = "columntodelete";
            columncreates[6].coltyp = JET_coltyp.Long;

            columncreates[7].szColumnName = "autoinc";
            columncreates[7].coltyp = JET_coltyp.Long;
            columncreates[7].grbit = ColumndefGrbit.ColumnAutoincrement;

            columncreates[8].szColumnName = "version";
            columncreates[8].coltyp = JET_coltyp.Long;
            columncreates[8].grbit = ColumndefGrbit.ColumnVersion;

            columncreates[9].szColumnName = "unicode";
            columncreates[9].coltyp = JET_coltyp.LongText;
            columncreates[9].cp = JET_CP.Unicode;
            columncreates[9].pvDefault = Encoding.Unicode.GetBytes(
                "This is the default value for the unicode column");
            columncreates[9].cbDefault = columncreates[9].pvDefault.Length;

            columncreates[10].szColumnName = "ascii";
            columncreates[10].coltyp = JET_coltyp.LongText;
            columncreates[10].cp = JET_CP.ASCII;
            columncreates[10].pvDefault = Encoding.ASCII.GetBytes("This is the default value for the ASCII column");
            columncreates[10].cbDefault = columncreates[10].pvDefault.Length;

            columncreates[11].szColumnName = "columntodelete2";
            columncreates[11].coltyp = JET_coltyp.Long;

            columncreates[12].szColumnName = "fixed";
            columncreates[12].coltyp = VistaColtyp.LongLong;
            columncreates[12].grbit = ColumndefGrbit.ColumnFixed;

            var primarySpaceHints = new JET_SPACEHINTS();
            primarySpaceHints.ulInitialDensity = 100;
            primarySpaceHints.cbInitial = 512 * 1024;

            var secondarySpaceHints = new JET_SPACEHINTS();
            secondarySpaceHints.ulInitialDensity = 80;
            secondarySpaceHints.cbInitial = 96 * 1024;
            secondarySpaceHints.ulGrowth = 150;
            secondarySpaceHints.cbMinExtent = 64 * 1024;
            secondarySpaceHints.cbMaxExtent = 256 * 1024;

            var indexcreates = new JET_INDEXCREATE[14];
            for (int i = 0; i < indexcreates.Length; ++i)
            {
                indexcreates[i] = new JET_INDEXCREATE();
            }

            indexcreates[0].szIndexName = "index_recordID";
            indexcreates[0].szKey = "+recordID\0\0";
            indexcreates[0].cbKey = indexcreates[0].szKey.Length;
            indexcreates[0].grbit = CreateIndexGrbit.IndexPrimary;
            indexcreates[0].pSpaceHints = primarySpaceHints;

            indexcreates[1] = this.MakeIndexcreate("tagged");
            indexcreates[2] = this.MakeIndexcreate("separated_lv");
            indexcreates[3] = this.MakeIndexcreate("compressed_unicode");
            indexcreates[4] = this.MakeIndexcreate("compressed_ascii");
            indexcreates[5] = this.MakeIndexcreate("compressed_binary");
            indexcreates[6] = this.MakeIndexcreate("autoinc");
            indexcreates[7] = this.MakeIndexcreate("version");
            indexcreates[8] = this.MakeIndexcreate("unicode");
            indexcreates[9] = this.MakeIndexcreate("ascii");
            indexcreates[10] = this.MakeIndexcreate("fixed");

            indexcreates[11].szIndexName = "secondary";
            indexcreates[11].szKey = "+autoinc\0+compressed_unicode\0+recordID\0\0";
            indexcreates[11].cbKey = indexcreates[11].szKey.Length;
            indexcreates[11].grbit = CreateIndexGrbit.IndexUnique;
            indexcreates[11].pSpaceHints = secondarySpaceHints;

            indexcreates[12].szIndexName = "indextodelete";
            indexcreates[12].szKey = "+autoinc\0+recordID\0\0";
            indexcreates[12].cbKey = indexcreates[12].szKey.Length;

            indexcreates[13] = this.MakeIndexcreate("columntodelete2");

            var tablecreate = new JET_TABLECREATE();
            tablecreate.szTableName = this.table;
            tablecreate.ulPages = 1;
            tablecreate.ulDensity = 100;
            tablecreate.rgcolumncreate = columncreates;
            tablecreate.cColumns = tablecreate.rgcolumncreate.Length;
            tablecreate.rgindexcreate = indexcreates;
            tablecreate.cIndexes = tablecreate.rgindexcreate.Length;

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, tablecreate);
            Api.JetCloseTable(this.sesid, tablecreate.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }
    }
}