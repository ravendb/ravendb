//-----------------------------------------------------------------------
// <copyright file="DMLTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace BasicTest
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;

    internal class DmlTests
    {
        private const int NumRecords = 23;
        private readonly ColumnInfo[] columnInfos;
        private readonly string database;
        private readonly JET_DBID dbid;
        private readonly JET_INSTANCE instance;
        private readonly JET_SESID sesid;
        private readonly string table;

        public DmlTests(JET_INSTANCE instance, string database, string table)
        {
            this.table = table;
            this.database = database;

            this.instance = instance;
            Api.JetBeginSession(this.instance, out this.sesid, null, null);
            Api.JetAttachDatabase(this.sesid, this.database, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(this.sesid, this.database, null, out this.dbid, OpenDatabaseGrbit.None);

            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction(this.sesid);
            this.columnInfos = Api.GetTableColumns(this.sesid, tableid).ToArray();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // runs the tests, leaving the table with a known set of records
        public void Create()
        {
            Console.WriteLine("DML tests");

            // these methods update the table
            this.Insert(); // inserts numRecords records with seed=recordID
            this.Replace(); // replaces record with seed=recordID+100
            this.Delete();
            this.InsertCopy();
            this.UpdateLongValues();
            this.CreateTempTable();

            // set the table to a known state
            this.Reset(); // deletes all records and re-inserts numRecords with seed=recordID+100

            // navigation (read-only)
            this.PrereadKeys();
            this.Seek();
            this.SeekSecondaryIndex();
            this.GotoBookmark();
            this.GotoSecondaryIndexBookmark();
            this.RetrieveKey();
            this.RetrieveFromPrimaryBookmark();

            // transactions (the table isn't changed)
            this.Rollback();
            this.WriteConflict();

            // make sure we have the records we expect
            this.VerifyRecords();
        }

        public void Term()
        {
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.WaitLastLevel0Commit);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
        }

        // verifies the set of records created by Run()
        public void VerifyRecords()
        {
            Console.WriteLine("\tVerifying records");

            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetSetTableSequential(this.sesid, tableid, SetTableSequentialGrbit.None);

            Api.JetMove(this.sesid, tableid, JET_Move.First, MoveGrbit.None);
            for (int recordID = 1; recordID <= NumRecords; ++recordID)
            {
                byte[][] data = this.GenerateColumnData(recordID, recordID + 100);
                this.CheckColumns(this.sesid, tableid, data);
                try
                {
                    Api.JetMove(this.sesid, tableid, JET_Move.Next, MoveGrbit.None);
                    data = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
                    BasicClass.Assert(
                        NumRecords != recordID, "The last move should generate an EsentNoCurrentRecordException");
                }
                catch (EsentNoCurrentRecordException)
                {
                    BasicClass.Assert(
                        NumRecords == recordID,
                        "Only the last move should generate an EsentNoCurrentRecordException");
                }
            }

            Api.JetCloseTable(this.sesid, tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.WaitLastLevel0Commit);
        }

        // separated LV columns should always be set and should be forced to be separate
        private bool IsColumnSeparatedLV(ColumnInfo columnInfo)
        {
            return columnInfo.Name.StartsWith("separated");
        }

        // autoinc-type columns shouldn't be set or checked.
        private bool IsColumnAutoinc(ColumndefGrbit grbit)
        {
            return ColumndefGrbit.ColumnAutoincrement == (grbit & ColumndefGrbit.ColumnAutoincrement)
                    || ColumndefGrbit.ColumnEscrowUpdate == (grbit & ColumndefGrbit.ColumnAutoincrement)
                    || ColumndefGrbit.ColumnVersion == (grbit & ColumndefGrbit.ColumnVersion);
        }

        // generate a set of random data for a record using the given recordID and random number seed
        private byte[][] GenerateColumnData(int recordID, int seed)
        {
            var data = new byte[this.columnInfos.Length][];
            for (int i = 0; i < this.columnInfos.Length; ++i)
            {
                if (this.IsColumnAutoinc(this.columnInfos[i].Grbit))
                {
                    // don't overwrite autoinc columns
                    data[i] = null;
                }
                else if (String.Equals(
                    this.columnInfos[i].Name, "recordID", StringComparison.InvariantCultureIgnoreCase))
                {
                    // this is the primary index column, set it to the recordID
                    data[i] = BitConverter.GetBytes(recordID);
                }
                else if (this.IsColumnSeparatedLV(this.columnInfos[i]))
                {
                    var rand = new Random(seed + this.columnInfos[i].Name.GetHashCode());
                    data[i] = new byte[rand.Next(5, 1024)];
                    rand.NextBytes(data[i]);
                }
                else
                {
                    var rand = new Random(seed + this.columnInfos[i].Name.GetHashCode());

                    // get some data
                    if (0 == rand.Next(4))
                    {
                        // null column
                        data[i] = null;
                    }
                    else
                    {
                        data[i] = DataGenerator.GetRandomColumnData(
                            this.columnInfos[i].Coltyp, this.columnInfos[i].Cp, rand);
                    }
                }
            }

            return data;
        }

        // set columns in the record (the data array should match the columnInfos array)
        private void SetColumns(JET_SESID sesid, JET_TABLEID tableid, byte[][] data)
        {
            for (int i = 0; i < this.columnInfos.Length; ++i)
            {
                // don't overwrite autoinc columns
                if (!this.IsColumnAutoinc(this.columnInfos[i].Grbit))
                {
                    SetColumnGrbit grbit = SetColumnGrbit.None;
                    if (this.IsColumnSeparatedLV(this.columnInfos[i]))
                    {
                        grbit |= SetColumnGrbit.SeparateLV;
                    }

                    Api.SetColumn(sesid, tableid, this.columnInfos[i].Columnid, data[i], grbit);
                }
            }
        }

        // retrieves all the columns from the record with JetRetrieveColumn
        private byte[][] GetColumnsWithJetRetrieveColumn(
            JET_SESID sesid, JET_TABLEID tableid, RetrieveColumnGrbit grbit)
        {
            var data = new byte[this.columnInfos.Length][];
            for (int i = 0; i < this.columnInfos.Length; ++i)
            {
                data[i] = Api.RetrieveColumn(sesid, tableid, this.columnInfos[i].Columnid, grbit, null);
            }

            return data;
        }

        // retrieves all the columns from the record with JetRetrieveColumns
        private byte[][] GetColumnsWithJetRetrieveColumns(JET_SESID sesid, JET_TABLEID tableid)
        {
            JET_RETRIEVECOLUMN[] retrieveColumns =
                (from x in this.columnInfos select new JET_RETRIEVECOLUMN { columnid = x.Columnid, itagSequence = 1 }).ToArray();
            Api.JetRetrieveColumns(sesid, tableid, retrieveColumns, retrieveColumns.Length);

            var data = new byte[retrieveColumns.Length][];
            for (int i = 0; i < retrieveColumns.Length; ++i)
            {
                if (JET_wrn.BufferTruncated == retrieveColumns[i].err)
                {
                    data[i] = new byte[retrieveColumns[i].cbActual];
                    retrieveColumns[i].pvData = data[i];
                    retrieveColumns[i].cbData = retrieveColumns[i].pvData.Length;
                }
            }

            Api.JetRetrieveColumns(sesid, tableid, retrieveColumns, retrieveColumns.Length);
            return data;
        }

        private byte[][] GetColumnsWithJetRetrieveColumn(JET_SESID sesid, JET_TABLEID tableid)
        {
            return this.GetColumnsWithJetRetrieveColumn(sesid, tableid, RetrieveColumnGrbit.None);
        }

        // compares two byte arrays
        private void CompareData(byte[] expectedData, byte[] actualData)
        {
            if (expectedData == null && actualData == null)
            {
            }
            else if (expectedData.Length != actualData.Length)
            {
                Console.WriteLine(
                    "ERROR: expected data is {0} bytes, but actual data is {1} bytes",
                    expectedData.Length,
                    actualData.Length);
                throw new Exception("Data mismatch");
            }
            else
            {
                for (int i = 0; i < expectedData.Length; ++i)
                {
                    if (expectedData[i] != actualData[i])
                    {
                        Console.WriteLine(
                            "ERROR: data differs at byte {0}. Expected {1:X2}, actual {2:X2}",
                            i,
                            expectedData[i],
                            actualData[i]);
                        throw new Exception("Data mismatch");
                    }
                }
            }
        }

        // compare the two data arrays
        private void CompareColumns(byte[][] expectedData, byte[][] actualData)
        {
            for (int i = 0; i < this.columnInfos.Length; ++i)
            {
                // don't check autoinc columns
                if (!this.IsColumnAutoinc(this.columnInfos[i].Grbit))
                {
                    this.CompareData(expectedData[i], actualData[i]);
                }
            }
        }

        // verifies that the data in the record matches the expected values
        private void CheckColumns(JET_SESID sesid, JET_TABLEID tableid, byte[][] expectedData)
        {
            byte[][] actualData;

            actualData = this.GetColumnsWithJetRetrieveColumn(sesid, tableid);
            this.CompareColumns(actualData, expectedData);
            actualData = this.GetColumnsWithJetRetrieveColumns(sesid, tableid);
            this.CompareColumns(actualData, expectedData);

            int totalsize = 0;
            foreach (var columndata in actualData)
            {
                if (columndata != null)
                {
                    totalsize += columndata.Length;
                }
            }

            // BUGBUG: need to investigate why this gives a value which is larger than the sum of the column sizes
            var recsize = new JET_RECSIZE();
            VistaApi.JetGetRecordSize(sesid, tableid, ref recsize, GetRecordSizeGrbit.None);
            BasicClass.Assert(
                recsize.cbData + recsize.cbLongValueData >= totalsize,
                String.Format("JetGetRecordSize returned {0} bytes, expected {1}", recsize.cbData + recsize.cbLongValueData, totalsize));
        }

        // find a record with the given recordID
        private void SeekRecord(JET_SESID sesid, JET_TABLEID tableid, int recordID)
        {
            Api.MakeKey(sesid, tableid, recordID, MakeKeyGrbit.NewKey);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekEQ);
        }

        // insert numRecords records with recordID = [1..numRecords] and seed = recordID
        private void Insert()
        {
            Console.WriteLine("\tInsert");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            // note that the recordIDs are 1-10 NOT 0-9
            Api.JetBeginTransaction(this.sesid);
            for (int recordID = 1; recordID <= NumRecords; ++recordID)
            {
                byte[][] data = this.GenerateColumnData(recordID, recordID);

                Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
                this.SetColumns(this.sesid, tableid, data);
                Api.JetUpdate(this.sesid, tableid);
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);
            Api.JetMove(this.sesid, tableid, JET_Move.First, MoveGrbit.None);
            for (int i = 1; i <= NumRecords; ++i)
            {
                byte[][] data = this.GenerateColumnData(i, i);
                this.CheckColumns(this.sesid, tableid, data);
                try
                {
                    Api.JetMove(this.sesid, tableid, JET_Move.Next, MoveGrbit.None);
                    BasicClass.Assert(NumRecords != i, "The last move should generate an EsentNoCurrentRecordException");
                }
                catch (EsentNoCurrentRecordException)
                {
                    BasicClass.Assert(
                        NumRecords == i, "Only the last move should generate an EsentNoCurrentRecordException");
                }
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // replaces records [1..numRecords] with seed = recordID+100
        private void Replace()
        {
            Console.WriteLine("\tReplace");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            Api.JetBeginTransaction(this.sesid);
            for (int i = NumRecords; i > 0; --i)
            {
                byte[][] data = this.GenerateColumnData(i, 100 + i);

                this.SeekRecord(this.sesid, tableid, i);
                byte[][] originaldata = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
                Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Replace);
                this.SetColumns(this.sesid, tableid, data);

                // the copy buffer should contain the new data
                byte[][] actualdata = this.GetColumnsWithJetRetrieveColumn(
                    this.sesid, tableid, RetrieveColumnGrbit.RetrieveCopy);
                this.CompareColumns(data, actualdata);

                // we haven't called update yet so this should be the original data
                actualdata = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
                this.CompareColumns(originaldata, actualdata);

                Api.JetUpdate(this.sesid, tableid);
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);
            Api.JetMove(this.sesid, tableid, JET_Move.Last, MoveGrbit.None);
            for (int i = NumRecords; i > 0; --i)
            {
                byte[][] data = this.GenerateColumnData(i, 100 + i);

                this.CheckColumns(this.sesid, tableid, data);
                try
                {
                    Api.JetMove(this.sesid, tableid, JET_Move.Previous, MoveGrbit.None);
                    BasicClass.Assert(1 != i, "The last move should generate an EsentNoCurrentRecordException");
                }
                catch (EsentNoCurrentRecordException)
                {
                    BasicClass.Assert(1 == i, "Only the last move should generate an EsentNoCurrentRecordException");
                }
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // Inserts a record and then deletes it
        private void Delete()
        {
            Console.WriteLine("\tDelete");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            int recordID = 450;

            byte[][] data = this.GenerateColumnData(recordID, recordID);
            Api.JetMove(this.sesid, tableid, JET_Move.Last, MoveGrbit.None);

            Api.JetBeginTransaction(this.sesid);
            using (var update = new Update(this.sesid, tableid, JET_prep.InsertCopy))
            {
                this.SetColumns(this.sesid, tableid, data);
                update.SaveAndGotoBookmark();
            }

            this.CheckColumns(this.sesid, tableid, data);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            this.SeekRecord(this.sesid, tableid, recordID);
            Api.JetDelete(this.sesid, tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            try
            {
                this.SeekRecord(this.sesid, tableid, recordID);
                throw new Exception("Expected an EsentRecordNotFoundException");
            }
            catch (EsentRecordNotFoundException)
            {
            }

            Api.JetCloseTable(this.sesid, tableid);
        }

        // Inserts a copy of a record and makes sure the data is equal
        private void InsertCopy()
        {
            Console.WriteLine("\tInsertCopy");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction(this.sesid);
            this.SeekRecord(this.sesid, tableid, NumRecords / 2);
            byte[][] data = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);

            using (var update = new Update(this.sesid, tableid, JET_prep.InsertCopy))
            {
                for (int i = 0; i < this.columnInfos.Length; ++i)
                {
                    int recordID = NumRecords + 13;
                    if (String.Equals(
                        this.columnInfos[i].Name, "recordID", StringComparison.InvariantCultureIgnoreCase))
                    {
                        // this is the primary index column, set it to the new recordID
                        data[i] = BitConverter.GetBytes(recordID);
                        Api.SetColumn(this.sesid, tableid, this.columnInfos[i].Columnid, data[i]);
                    }
                }

                update.SaveAndGotoBookmark();
            }

            this.CheckColumns(this.sesid, tableid, data);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // insert a record and update its long-values
        private void UpdateLongValues()
        {
            Console.WriteLine("\tUpdate Long-Values");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            int recordID = NumRecords + 17;
            var rand = new Random(recordID);

            var data = new byte[this.columnInfos.Length][];

            Api.JetBeginTransaction(this.sesid);

            // insert the record
            using (var update = new Update(this.sesid, tableid, JET_prep.Insert))
            {
                for (int i = 0; i < this.columnInfos.Length; ++i)
                {
                    data[i] = null;
                    if (String.Equals(
                        this.columnInfos[i].Name, "recordID", StringComparison.InvariantCultureIgnoreCase))
                    {
                        // this is the primary index column, set it to the recordID
                        data[i] = BitConverter.GetBytes(recordID);
                    }
                    else if (this.columnInfos[i].Coltyp == JET_coltyp.LongBinary
                             || this.columnInfos[i].Coltyp == JET_coltyp.LongText)
                    {
                        data[i] = DataGenerator.GetRandomColumnData(
                            this.columnInfos[i].Coltyp, this.columnInfos[i].Cp, rand);
                    }

                    if (null != data[i])
                    {
                        Api.SetColumn(this.sesid, tableid, this.columnInfos[i].Columnid, data[i]);
                    }
                }

                update.SaveAndGotoBookmark();
            }

            this.CheckColumns(this.sesid, tableid, data);

            // update the record
            using (var update = new Update(this.sesid, tableid, JET_prep.Replace))
            {
                for (int i = 0; i < this.columnInfos.Length; ++i)
                {
                    if (this.columnInfos[i].Coltyp == JET_coltyp.LongBinary
                        || this.columnInfos[i].Coltyp == JET_coltyp.LongText)
                    {
                        int size = Api.RetrieveColumnSize(this.sesid, tableid, this.columnInfos[i].Columnid).Value;
                        BasicClass.Assert(size == data[i].Length, "Invalid column size");

                        var setinfo = new JET_SETINFO();
                        setinfo.ibLongValue = size / 2;
                        setinfo.itagSequence = 1;

                        // the data that will be added to the column
                        byte[] newdata = DataGenerator.GetRandomColumnData(
                            this.columnInfos[i].Coltyp, this.columnInfos[i].Cp, rand);

                        // what the final data should be
                        byte[] finaldata = null;

                        switch (rand.Next(2))
                        {
                            case 0: // append
                                Api.SetColumn(
                                    this.sesid, tableid, this.columnInfos[i].Columnid, newdata, SetColumnGrbit.AppendLV);
                                finaldata = new byte[size + newdata.Length];
                                Array.Copy(data[i], finaldata, size);
                                Array.Copy(newdata, 0, finaldata, size, newdata.Length);
                                break;
                            case 1: // overwrite and set size
                                Api.JetSetColumn(
                                    this.sesid,
                                    tableid,
                                    this.columnInfos[i].Columnid,
                                    newdata,
                                    newdata.Length,
                                    SetColumnGrbit.SizeLV | SetColumnGrbit.OverwriteLV,
                                    setinfo);
                                finaldata = new byte[setinfo.ibLongValue + newdata.Length];
                                Array.Copy(data[i], finaldata, setinfo.ibLongValue);
                                Array.Copy(newdata, 0, finaldata, setinfo.ibLongValue, newdata.Length);
                                break;
                        }

                        data[i] = finaldata;
                    }
                }

                update.SaveAndGotoBookmark();
            }

            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // create a temp table and insert some records
        private void CreateTempTable()
        {
            Console.WriteLine("\tTemporary Table");

            Api.JetBeginTransaction(this.sesid);

            var ci = new CultureInfo("en-us");

            var tt = new JET_OPENTEMPORARYTABLE();
            tt.prgcolumndef = new JET_COLUMNDEF[2];
            tt.ccolumn = 2;
            tt.pidxunicode = new JET_UNICODEINDEX();
            tt.pidxunicode.lcid = ci.LCID;
            tt.pidxunicode.dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreCase);
            tt.grbit = TempTableGrbit.Indexed;

            tt.prgcolumndef[0] = new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey };
            tt.prgcolumndef[1] = new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.LongText,
                cp = JET_CP.Unicode,
                grbit = ColumndefGrbit.TTKey
            };

            tt.prgcolumnid = new JET_COLUMNID[tt.prgcolumndef.Length];

            VistaApi.JetOpenTemporaryTable(this.sesid, tt);
            JET_TABLEID tableid = tt.tableid;

            for (int i = 0; i <= 5; ++i)
            {
                int key = 5 - i;
                string s = String.Format("Record {0}", i);

                Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
                Api.SetColumn(this.sesid, tableid, tt.prgcolumnid[0], key);
                Api.SetColumn(this.sesid, tableid, tt.prgcolumnid[1], s, Encoding.Unicode);
                Api.JetUpdate(this.sesid, tableid);
            }

            int expectedKey = 0;
            Api.MoveBeforeFirst(this.sesid, tableid);
            while (Api.TryMoveNext(this.sesid, tableid))
            {
                int actualKey = Api.RetrieveColumnAsInt32(this.sesid, tableid, tt.prgcolumnid[0]).Value;
                BasicClass.Assert(
                    expectedKey == actualKey,
                    String.Format("Temp table isn't sorted correctly (expected = {0}, actual = {1})", expectedKey, actualKey));
                expectedKey++;
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // delete all records in the table and then insert [1..numRecords] records
        // with seed = recordID+100
        private void Reset()
        {
            Console.WriteLine("\tReset");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            Api.JetBeginTransaction(this.sesid);
            Api.JetMove(this.sesid, tableid, JET_Move.Last, MoveGrbit.None);
            do
            {
                Api.JetDelete(this.sesid, tableid);
            }
            while (Api.TryMovePrevious(this.sesid, tableid));
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            for (int recordID = NumRecords; recordID > 0; --recordID)
            {
                byte[][] data = this.GenerateColumnData(recordID, recordID + 100);

                Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
                this.SetColumns(this.sesid, tableid, data);
                Api.JetUpdate(this.sesid, tableid);
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetCloseTable(this.sesid, tableid);
        }

        // update a record and then rollback
        private void Rollback()
        {
            Console.WriteLine("\tRollback");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            int recordID = 1;

            byte[][] data = this.GenerateColumnData(recordID, recordID + 200);

            Api.JetBeginTransaction(this.sesid);
            this.SeekRecord(this.sesid, tableid, recordID);

            Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Replace);
            this.SetColumns(this.sesid, tableid, data);
            Api.JetUpdate(this.sesid, tableid);

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);

            data = this.GenerateColumnData(recordID, recordID + 100);

            this.SeekRecord(this.sesid, tableid, recordID);
            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCloseTable(this.sesid, tableid);
        }

        // try different seek options (SeekLT and SeekGT)
        private void Seek()
        {
            Console.WriteLine("\tSeek");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            int recordID = 5;
            int recordIDExpected;
            byte[][] data;

            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            // SeekLT
            Api.MakeKey(this.sesid, tableid, recordID, MakeKeyGrbit.NewKey);
            Api.JetSeek(this.sesid, tableid, SeekGrbit.SeekLT);
            recordIDExpected = recordID - 1;
            data = this.GenerateColumnData(recordIDExpected, recordIDExpected + 100);
            this.CheckColumns(this.sesid, tableid, data);

            // SeekGT
            Api.MakeKey(this.sesid, tableid, recordID, MakeKeyGrbit.NewKey);
            Api.JetSeek(this.sesid, tableid, SeekGrbit.SeekGT);
            recordIDExpected = recordID + 1;
            data = this.GenerateColumnData(recordIDExpected, recordIDExpected + 100);
            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        private void PrereadKeys()
        {
            Console.WriteLine("\tPrereadKeys");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            var keys = new byte[4][];

            for (int i = 0; i < keys.Length; ++i)
            {
                Api.MakeKey(this.sesid, tableid, i, MakeKeyGrbit.NewKey);
                keys[i] = Api.RetrieveKey(this.sesid, tableid, RetrieveKeyGrbit.RetrieveCopy);
            }

            int[] keyLengths = (from x in keys select x.Length).ToArray();

            int ignored;
            Windows7Api.JetPrereadKeys(
                this.sesid, tableid, keys, keyLengths, keys.Length, out ignored, PrereadKeysGrbit.Forward);

            Api.JetCloseTable(this.sesid, tableid);
        }

        // seek and use index ranges on secondary indexes
        private bool CompareByteArray(byte[] a, byte[] b, int length)
        {
            if (a == b)
            {
                return true;
            }

            for (int i = 0; i < length; ++i)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }

            return true;
        }

        private void SeekSecondaryIndex(JET_SESID sesid, JET_TABLEID tableid)
        {
            byte[][] data = this.GetColumnsWithJetRetrieveColumn(sesid, tableid);
            byte[] bookmark = Api.GetBookmark(sesid, tableid);
            for (int i = 0; i < this.columnInfos.Length; ++i)
            {
                string index = String.Format("index_{0}", this.columnInfos[i].Name);

                Api.JetSetCurrentIndex(sesid, tableid, index);
                string actualindex;
                Api.JetGetCurrentIndex(sesid, tableid, out actualindex, SystemParameters.NameMost);
                BasicClass.Assert(
                    String.Equals(actualindex, index, StringComparison.InvariantCultureIgnoreCase),
                    String.Format("Set index to {0}, JetGetCurrentIndex returns {1}", index, actualindex));

                // create an index range that will contain the record we want
                Api.MakeKey(sesid, tableid, data[i], MakeKeyGrbit.NewKey);
                Api.JetSeek(sesid, tableid, SeekGrbit.SeekEQ);

                Api.MakeKey(sesid, tableid, data[i], MakeKeyGrbit.StrLimit | MakeKeyGrbit.NewKey);
                Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeUpperLimit);

                // now move through the range until we find the record we want
                bool foundrecord = false;
                do
                {
                    // verify we have the same column value. don't retrieve the entire column as we
                    // have to deal with key truncation
                    byte[] columndata = Api.RetrieveColumn(
                        sesid, tableid, this.columnInfos[i].Columnid, RetrieveColumnGrbit.RetrieveFromIndex, null);
                    BasicClass.Assert(
                        this.CompareByteArray(columndata, data[i], null == columndata ? 0 : columndata.Length),
                        "Unexpected column value. Did we go past the end of the index range?");

                    if (this.CompareByteArray(bookmark, Api.GetBookmark(sesid, tableid), bookmark.Length))
                    {
                        BasicClass.Assert(!foundrecord, "Found the record twice");
                        foundrecord = true;
                    }
                }
                while (Api.TryMoveNext(sesid, tableid));
                BasicClass.Assert(foundrecord, "Didn't find the record in the secondary index");
            }
        }

        private void SeekSecondaryIndex()
        {
            Console.WriteLine("\tSeek on secondary indexes");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            JET_TABLEID tableid2;
            Api.JetDupCursor(this.sesid, tableid, out tableid2, DupCursorGrbit.None);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            Api.JetMove(this.sesid, tableid, JET_Move.First, MoveGrbit.None);
            do
            {
                // as the currency and current index will be changed, use a different cursor
                byte[] bookmark = Api.GetBookmark(this.sesid, tableid);
                Api.JetGotoBookmark(this.sesid, tableid2, bookmark, bookmark.Length);
                this.SeekSecondaryIndex(this.sesid, tableid2);
            }
            while (Api.TryMoveNext(this.sesid, tableid));

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // get a bookmark, move to a different record and then go back to the bookmark
        private void GotoBookmark()
        {
            Console.WriteLine("\tGotoBookmark");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            this.SeekRecord(this.sesid, tableid, 3);
            byte[] bookmark = Api.GetBookmark(this.sesid, tableid);
            byte[][] data = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
            this.SeekRecord(this.sesid, tableid, 1);
            Api.JetGotoBookmark(this.sesid, tableid, bookmark, bookmark.Length);
            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // get a bookmark, move to a different record and then go back to the bookmark
        private void GotoSecondaryIndexBookmark()
        {
            Console.WriteLine("\tGotoSecondaryIndexBookmark");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            this.SeekRecord(this.sesid, tableid, 5);
            byte[][] data = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
            Api.JetSetCurrentIndex2(this.sesid, tableid, "secondary", SetCurrentIndexGrbit.NoMove);

            var secondaryKey = new byte[SystemParameters.KeyMost];
            int secondaryKeySize;
            var primaryKey = new byte[SystemParameters.KeyMost];
            int primaryKeySize;

            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                tableid,
                secondaryKey,
                secondaryKey.Length,
                out secondaryKeySize,
                primaryKey,
                primaryKey.Length,
                out primaryKeySize,
                GetSecondaryIndexBookmarkGrbit.None);
            Api.JetMove(this.sesid, tableid, JET_Move.Next, MoveGrbit.None);
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                tableid,
                secondaryKey,
                secondaryKeySize,
                primaryKey,
                primaryKeySize,
                GotoSecondaryIndexBookmarkGrbit.None);
            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // get a key, move to a different record and then go back to the key
        private void RetrieveKey()
        {
            Console.WriteLine("\tRetrieveKey");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            this.SeekRecord(this.sesid, tableid, 4);
            byte[][] data = this.GetColumnsWithJetRetrieveColumn(this.sesid, tableid);
            byte[] key = Api.RetrieveKey(this.sesid, tableid, RetrieveKeyGrbit.None);

            this.SeekRecord(this.sesid, tableid, 2);

            Api.MakeKey(this.sesid, tableid, key, MakeKeyGrbit.NormalizedKey);
            Api.JetSeek(this.sesid, tableid, SeekGrbit.SeekEQ);
            this.CheckColumns(this.sesid, tableid, data);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // retrieve the key column with JET_bitRetrieveFromPrimaryBookmark
        private void RetrieveFromPrimaryBookmark()
        {
            Console.WriteLine("\tRetrieveFromPrimaryBookmark");
            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            int expectedRecordID = 7;
            this.SeekRecord(this.sesid, tableid, expectedRecordID);
            JET_COLUMNID columnid =
                (from column in this.columnInfos
                 where column.Name == "recordID"
                 select column.Columnid).Single();
            byte[] columndata = Api.RetrieveColumn(
                this.sesid, tableid, columnid, RetrieveColumnGrbit.RetrieveFromPrimaryBookmark, null);
            int actualRecordID = BitConverter.ToInt32(columndata, 0);
            BasicClass.Assert(
                expectedRecordID == actualRecordID,
                String.Format("RetrieveFromPrimaryBookmark: got {0}, expected {1}", actualRecordID, expectedRecordID));

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(this.sesid, tableid);
        }

        // create a second session to test the write conflict code
        private void WriteConflict()
        {
            Console.WriteLine("\tWriteConflict");

            JET_SESID sesid2;
            Api.JetDupSession(this.sesid, out sesid2);
            JET_DBID dbid2;
            Api.JetOpenDatabase(sesid2, this.database, null, out dbid2, OpenDatabaseGrbit.None);

            JET_TABLEID tableid;
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out tableid);

            string name;
            Api.JetGetTableInfo(this.sesid, tableid, out name, JET_TblInfo.Name);
            JET_TABLEID tableid2;
            Api.JetOpenTable(sesid2, dbid2, name, null, 0, OpenTableGrbit.None, out tableid2);

            // insert a record
            int recordID = 999;
            Api.JetBeginTransaction(this.sesid);
            byte[][] originaldata = this.GenerateColumnData(recordID, recordID);
            Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
            this.SetColumns(this.sesid, tableid, originaldata);
            Api.JetUpdate(this.sesid, tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            // update the record with the first session, but don't commit
            Api.JetBeginTransaction(this.sesid);
            byte[][] newdata = this.GenerateColumnData(recordID, recordID + 100);
            this.SeekRecord(this.sesid, tableid, recordID);
            Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Replace);
            this.SetColumns(this.sesid, tableid, newdata);
            Api.JetUpdate(this.sesid, tableid);
            this.CheckColumns(this.sesid, tableid, newdata);

            // now try to delete with the second transaction
            // make sure the second transaction doesn't see the first transaction's updates
            Api.JetBeginTransaction(sesid2);
            this.SeekRecord(sesid2, tableid2, recordID);
            this.CheckColumns(sesid2, tableid2, originaldata);
            try
            {
                Api.JetDelete(sesid2, tableid2);
                throw new Exception("Expected an EsentWriteConflictException");
            }
            catch (EsentWriteConflictException)
            {
            }

            Api.JetRollback(sesid2, RollbackTransactionGrbit.None);

            // now commit the first transaction and start a new one
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);

            // now delete with the second transaction
            Api.JetBeginTransaction(sesid2);
            this.SeekRecord(sesid2, tableid2, recordID);
            Api.JetDelete(sesid2, tableid2);
            Api.JetCommitTransaction(sesid2, CommitTransactionGrbit.None);

            // make sure the first transaction can still see the record
            this.SeekRecord(this.sesid, tableid, recordID);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            // close tables and end the extra session
            Api.JetCloseTable(this.sesid, tableid);
            Api.JetCloseTable(sesid2, tableid2);
            Api.JetEndSession(sesid2, EndSessionGrbit.None);
        }
    }
}