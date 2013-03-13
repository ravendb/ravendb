//-----------------------------------------------------------------------
// <copyright file="TempTableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace BasicTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// The temp table tests.
    /// </summary>
    internal class TempTableTests
    {
        /// <summary>
        /// The coltyps being used for the test.
        /// </summary>
        private readonly JET_coltyp[] coltyps = new[]
        {
            // the long column must come first! it is indexed and checked
            JET_coltyp.Long,
            JET_coltyp.Text,
            JET_coltyp.UnsignedByte,
            JET_coltyp.LongBinary,
            JET_coltyp.Short,
            JET_coltyp.Currency,
            JET_coltyp.IEEESingle,
            JET_coltyp.Bit,
            JET_coltyp.IEEEDouble,
            JET_coltyp.DateTime,
            JET_coltyp.Binary,
            JET_coltyp.LongText,
            ////VistaColtyp.UnsignedLong,
            ////VistaColtyp.LongLong,
            ////VistaColtyp.GUID,
            ////VistaColtyp.UnsignedShort,
        };

        /// <summary>
        /// The Jet instance being used for the test.
        /// </summary>
        private readonly JET_INSTANCE instance;

        /// <summary>
        /// Initializes a new instance of the <see cref="TempTableTests"/> class.
        /// </summary>
        /// <param name="instance">
        /// The instance.
        /// </param>
        public TempTableTests(JET_INSTANCE instance)
        {
            this.instance = instance;
        }

        /// <summary>
        /// Run the test.
        /// </summary>
        public void Run()
        {
            JET_SESID sesid;
            Api.JetBeginSession(this.instance, out sesid, null, null);

            Console.WriteLine("Temporary table tests");

            Api.JetBeginTransaction(sesid);

            var ci = new CultureInfo("en-us");
            var tt = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef =
                    (from coltyp in this.coltyps select new JET_COLUMNDEF { coltyp = coltyp, cp = JET_CP.Unicode }).
                    ToArray(),
                pidxunicode = new JET_UNICODEINDEX
                {
                    lcid = ci.LCID,
                    dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreCase)
                },
                grbit = TempTableGrbit.Indexed,
            };

            tt.ccolumn = tt.prgcolumndef.Length;
            tt.prgcolumnid = new JET_COLUMNID[tt.prgcolumndef.Length];
            tt.prgcolumndef[0].grbit = ColumndefGrbit.TTKey;
            tt.prgcolumndef[1].grbit = ColumndefGrbit.TTKey | ColumndefGrbit.TTDescending;
            tt.prgcolumndef[2].grbit = ColumndefGrbit.TTKey;
            tt.prgcolumndef[3].grbit = ColumndefGrbit.TTKey | ColumndefGrbit.TTDescending;

            VistaApi.JetOpenTemporaryTable(sesid, tt);

            int numrecords = 500;

            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            foreach (int i in Randomize(Enumerable.Range(0, numrecords)))
            {
                using (var update = new Update(sesid, tt.tableid, JET_prep.Insert))
                {
                    for (int j = 0; j < tt.prgcolumndef.Length; ++j)
                    {
                        Api.SetColumn(
                            sesid,
                            tt.tableid,
                            tt.prgcolumnid[j],
                            DataGenerator.GetRandomColumnData(tt.prgcolumndef[j].coltyp, tt.prgcolumndef[j].cp, rand));
                    }

                    // overwrite the first column, which is an integer key. this will be used to validate
                    // the sorting of the objects
                    Api.SetColumn(sesid, tt.tableid, tt.prgcolumnid[0], BitConverter.GetBytes(i));
                    update.Save();
                }
            }

            stopwatch.Stop();
            Console.WriteLine("\tInserted {0} records in {1}", numrecords, stopwatch.Elapsed);

            // iterate over the table to force materialization
            stopwatch = Stopwatch.StartNew();
            BasicClass.Assert(
                Enumerable.Range(0, numrecords).SequenceEqual(GetColumns(sesid, tt.tableid, tt.prgcolumnid[0])),
                "Didn't get expected keys");
            stopwatch.Stop();
            Console.WriteLine("\tRetrieved {0} records in {1}", numrecords, stopwatch.Elapsed);

            numrecords = 10000;
            stopwatch = Stopwatch.StartNew();
            IEnumerable<int> sortedData = Enumerable.Range(0, numrecords);
            BasicClass.Assert(
                sortedData.SequenceEqual(SortWithTempTable(sesid, Randomize(sortedData))),
                "Data isn't sorted");
            stopwatch.Stop();
            Console.WriteLine("\tSorted {0} numbers in {1}", numrecords, stopwatch.Elapsed);

            Console.WriteLine("\tSeeking");
            SeekWithTempTable(sesid);

            Api.JetCommitTransaction(sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetCloseTable(sesid, tt.tableid);
            Api.JetEndSession(sesid, EndSessionGrbit.None);
        }

        /// <summary>
        /// Iterate over all records in the table, returning column values.
        /// </summary>
        /// <param name="sesid">
        /// The sesssion.
        /// </param>
        /// <param name="tableid">
        /// The tableid.
        /// </param>
        /// <param name="columnid">
        /// The columnid.
        /// </param>
        /// <returns>
        /// An enumeration of the column on all records in the table.
        /// </returns>
        private static IEnumerable<int> GetColumns(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            Api.MoveBeforeFirst(sesid, tableid);
            while (Api.TryMoveNext(sesid, tableid))
            {
                yield return Api.RetrieveColumnAsInt32(sesid, tableid, columnid).Value;
            }
        }

        /// <summary>
        /// Sort data with a temp table.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="data">
        /// The data to sort.
        /// </param>
        /// <returns>
        /// An enumeration of the sorted data.
        /// </returns>
        private static IEnumerable<int> SortWithTempTable(JET_SESID sesid, IEnumerable<int> data)
        {
            var ci = new CultureInfo("en-us");
            var tt = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef = new[]
                {
                    new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.Long,
                        grbit = ColumndefGrbit.TTKey,
                    }
                },
                ccolumn = 1,
                prgcolumnid = new JET_COLUMNID[1],
                pidxunicode = new JET_UNICODEINDEX
                {
                    lcid = ci.LCID,
                    dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreCase),
                },
                grbit = TempTableGrbit.None,
            };

            VistaApi.JetOpenTemporaryTable(sesid, tt);
            foreach (int i in data)
            {
                Api.JetPrepareUpdate(sesid, tt.tableid, JET_prep.Insert);
                Api.SetColumn(sesid, tt.tableid, tt.prgcolumnid[0], i);
                Api.JetUpdate(sesid, tt.tableid);
            }

            try
            {
                Api.JetMove(sesid, tt.tableid, JET_Move.First, MoveGrbit.None);
                do
                {
                    yield return Api.RetrieveColumnAsInt32(sesid, tt.tableid, tt.prgcolumnid[0]).Value;
                }
                while (Api.TryMoveNext(sesid, tt.tableid));
            }
            finally
            {
                Api.JetCloseTable(sesid, tt.tableid);
            }
        }

        /// <summary>
        /// Seek for a key and make sure we land on the expected record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to seek on.</param>
        /// <param name="key">The key to seek for.</param>
        /// <param name="seekOption">The seek option.</param>
        /// <param name="columnid">The columnid of the data to retrieve.</param>
        /// <param name="expected">The expected data.</param>
        private static void VerifySeekFindRecord(
            JET_SESID sesid, JET_TABLEID tableid, int key, SeekGrbit seekOption, JET_COLUMNID columnid, int expected)
        {
            Console.WriteLine("\t\tSeek for {0} with {1}, expecting {2}", key, seekOption, expected);
            Api.MakeKey(sesid, tableid, key, MakeKeyGrbit.NewKey);

            Api.JetSeek(sesid, tableid, seekOption);
            int actual = Api.RetrieveColumnAsInt32(sesid, tableid, columnid).Value;
            BasicClass.Assert(expected == actual, String.Format("Expected {0}, got {1}. Seek is broken", expected, actual));
        }

        /// <summary>
        /// Seek for a key and make sure we don't find a record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to seek on.</param>
        /// <param name="key">The key to seek for.</param>
        /// <param name="seekOption">The seek option.</param>
        private static void VerifySeekFails(JET_SESID sesid, JET_TABLEID tableid, int key, SeekGrbit seekOption)
        {
            Console.WriteLine("\t\tSeek for {0} with {1}, expecting failure", key, seekOption);
            Api.MakeKey(sesid, tableid, key, MakeKeyGrbit.NewKey);
            bool result = Api.TrySeek(sesid, tableid, seekOption);
            BasicClass.Assert(!result, "Found the record. Expected not found.");
        }

        /// <summary>
        /// The seek with temp table.
        /// </summary>
        /// <param name="sesid">
        /// The sesid.
        /// </param>
        private static void SeekWithTempTable(JET_SESID sesid)
        {
            var tt = new JET_OPENTEMPORARYTABLE
            {
                ccolumn = 1,
                prgcolumndef = new[] { new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey } },
                prgcolumnid = new JET_COLUMNID[1],
                grbit = TempTableGrbit.Indexed,
            };

            VistaApi.JetOpenTemporaryTable(sesid, tt);

            // Insert records 0, 10, 20, 30, ... 90);
            foreach (int i in Enumerable.Range(0, 10))
            {
                Api.JetPrepareUpdate(sesid, tt.tableid, JET_prep.Insert);
                Api.SetColumn(sesid, tt.tableid, tt.prgcolumnid[0], i * 10);
                Api.JetUpdate(sesid, tt.tableid);
            }

            try
            {
                // Boundary: before start of table
                ////VerifySeekFails(sesid, tt.tableid, -1, SeekGrbit.SeekLT);
                ////VerifySeekFails(sesid, tt.tableid, -1, SeekGrbit.SeekLE);
                VerifySeekFails(sesid, tt.tableid, -1, SeekGrbit.SeekEQ);
                VerifySeekFindRecord(sesid, tt.tableid, -1, SeekGrbit.SeekGE, tt.prgcolumnid[0], 0);
                VerifySeekFindRecord(sesid, tt.tableid, -1, SeekGrbit.SeekGT, tt.prgcolumnid[0], 0);

                // Boundary: at start of table
                VerifySeekFails(sesid, tt.tableid, 0, SeekGrbit.SeekLT);
                VerifySeekFindRecord(sesid, tt.tableid, 0, SeekGrbit.SeekLE, tt.prgcolumnid[0], 0);
                VerifySeekFindRecord(sesid, tt.tableid, 0, SeekGrbit.SeekEQ, tt.prgcolumnid[0], 0);
                ////VerifySeekFindRecord(sesid, tt.tableid, 0, SeekGrbit.SeekGE, tt.prgcolumnid[0], 0);
                VerifySeekFindRecord(sesid, tt.tableid, 0, SeekGrbit.SeekGT, tt.prgcolumnid[0], 10);

                // Normal case: middle of table, key exists
                VerifySeekFindRecord(sesid, tt.tableid, 50, SeekGrbit.SeekLT, tt.prgcolumnid[0], 40);
                VerifySeekFindRecord(sesid, tt.tableid, 50, SeekGrbit.SeekLE, tt.prgcolumnid[0], 50);
                VerifySeekFindRecord(sesid, tt.tableid, 50, SeekGrbit.SeekEQ, tt.prgcolumnid[0], 50);
                ////VerifySeekFindRecord(sesid, tt.tableid, 50, SeekGrbit.SeekGE, tt.prgcolumnid[0], 50);
                VerifySeekFindRecord(sesid, tt.tableid, 50, SeekGrbit.SeekGT, tt.prgcolumnid[0], 60);

                // Normal case: middle of table, key doesn't exist
                ////VerifySeekFindRecord(sesid, tt.tableid, 75, SeekGrbit.SeekLT, tt.prgcolumnid[0], 70);
                ////VerifySeekFindRecord(sesid, tt.tableid, 75, SeekGrbit.SeekLE, tt.prgcolumnid[0], 70);
                VerifySeekFails(sesid, tt.tableid, 75, SeekGrbit.SeekEQ);
                VerifySeekFindRecord(sesid, tt.tableid, 75, SeekGrbit.SeekGE, tt.prgcolumnid[0], 80);
                VerifySeekFindRecord(sesid, tt.tableid, 75, SeekGrbit.SeekGT, tt.prgcolumnid[0], 80);

                // Boundary: at end of table
                VerifySeekFindRecord(sesid, tt.tableid, 90, SeekGrbit.SeekLT, tt.prgcolumnid[0], 80);
                VerifySeekFindRecord(sesid, tt.tableid, 90, SeekGrbit.SeekLE, tt.prgcolumnid[0], 90);
                VerifySeekFindRecord(sesid, tt.tableid, 90, SeekGrbit.SeekEQ, tt.prgcolumnid[0], 90);
                ////VerifySeekFindRecord(sesid, tt.tableid, 90, SeekGrbit.SeekGE, tt.prgcolumnid[0], 90);
                VerifySeekFails(sesid, tt.tableid, 90, SeekGrbit.SeekGT);

                // Boundary: past end of table
                ////VerifySeekFindRecord(sesid, tt.tableid, 99, SeekGrbit.SeekLT, tt.prgcolumnid[0], 90);
                ////VerifySeekFindRecord(sesid, tt.tableid, 99, SeekGrbit.SeekLE, tt.prgcolumnid[0], 90);
                VerifySeekFails(sesid, tt.tableid, 99, SeekGrbit.SeekEQ);
                VerifySeekFails(sesid, tt.tableid, 99, SeekGrbit.SeekGE);
                VerifySeekFails(sesid, tt.tableid, 99, SeekGrbit.SeekGT);
            }
            finally
            {
                Api.JetCloseTable(sesid, tt.tableid);
            }
        }

        /// <summary>
        /// Produce the elements from the input in random order
        /// </summary>
        /// <typeparam name="T">The type in the sequence.</typeparam>
        /// <param name="sequence">The sequence to randomize.</param>
        /// <returns>The elements of the sequence, in random order.</returns>
        private static IEnumerable<T> Randomize<T>(IEnumerable<T> sequence)
        {
            T[] data = sequence.ToArray();
            var rand = new Random();
            for (int i = 0; i < data.Length; ++i)
            {
                // for sequence randomization we have to swap the members, but 
                // here we just need to return it
                int j = rand.Next(i, data.Length);
                T temp = data[j];
                data[j] = data[i];
                yield return temp;
            }
        }
    }
}