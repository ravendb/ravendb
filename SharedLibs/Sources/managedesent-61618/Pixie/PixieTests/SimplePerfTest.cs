//-----------------------------------------------------------------------
// <copyright file="SimplePerfTest.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Basic performance tests
    /// </summary>
    [TestClass]
    public class SimplePerfTest
    {
        const int DataSize = 32;

        private Connection connection;
        private Table table;

        // Used to insert records
        private long nextKey = 0;
        private byte[] data;

        private Random random;

        /// <summary>
        /// Setup for a test -- this creates the database
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.random = new Random();
            this.data = new byte[DataSize];
            this.random.NextBytes(this.data);

            this.connection = Esent.CreateDatabase("pixie\\esentperftest.db");

            // Create the table
            this.table = connection.CreateTable("table");
            using (var trx = connection.BeginTransaction())
            {
                table.CreateColumn(DefinedAs.Int64Column("key"));
                table.CreateColumn(DefinedAs.BinaryColumn("data"));
                trx.Commit();
            }
        }

        /// <summary>
        /// Cleanup after the test
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            this.connection.Dispose();
        }

        /// <summary>
        /// Test inserting and retrieving records.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void BasicPerfTest()
        {
            this.CheckMemoryUsage(this.InsertReadSeek);
        }

        private void InsertReadSeek()
        {
            const int NumRecords = 1000000;

            // Randomly seek to all records in the table
            long[] keys = (from x in Enumerable.Range(0, NumRecords) select (long)x).ToArray();
            this.Shuffle(keys);

            TimeAction("Insert records", () => this.InsertRecords(NumRecords));
            TimeAction("Read one record", () => this.RepeatedlyRetrieveOneRecord(NumRecords));
            TimeAction("Read all records", this.RetrieveAllRecords);
            TimeAction("Seek to all records", () => this.SeekToAllRecords(keys));
        }

        private void CheckMemoryUsage(Action action)
        {
            RunGarbageCollection();
            long memoryAtStart = GC.GetTotalMemory(true);

            action();

            RunGarbageCollection();
            long memoryAtEnd = GC.GetTotalMemory(true);
            Console.WriteLine("Memory changed by {0} bytes", memoryAtEnd - memoryAtStart);
        }

        private void Shuffle<T>(T[] arrayToShuffle)
        {
            for (int i = 0; i < arrayToShuffle.Length; ++i)
            {
                int swap = this.random.Next(i, arrayToShuffle.Length);
                T temp = arrayToShuffle[i];
                arrayToShuffle[i] = arrayToShuffle[swap];
                arrayToShuffle[swap] = temp;
            }
        }

        private static void TimeAction(string name, Action action)
        {
            var stopwatch = Stopwatch.StartNew();
            action();
            stopwatch.Stop();
            Console.WriteLine("{0}: {1}", name, stopwatch.Elapsed);
        }

        private void InsertRecord()
        {
            this.table.NewRecord()
                .SetColumn("key", this.nextKey++)
                .SetColumn("data", this.data)
                .Save();
        }

        private void InsertRecords(int numRecords)
        {
            for (int i = 0; i < numRecords; ++i)
            {
                this.connection.UsingLazyTransaction(this.InsertRecord);
            }
        }

        private void RetrieveAllRecords()
        {
            foreach (Record record in this.table)
            {
                RetrieveRecordColumns(record);
            }
        }

        private static void RetrieveRecordColumns(Record record)
        {
            long temp1 = (long)record["key"];
            byte[] temp2 = (byte[])record["data"];
        }

        private void RepeatedlyRetrieveOneRecord(int numRetrieves)
        {
            Record record = this.table.First();
            for (int i = 0; i < numRetrieves; ++i)
            {
                this.connection.UsingTransaction(() => RetrieveRecordColumns(record));
            }
        }

        private void SeekToAllRecords(long[] keys)
        {
            foreach (long key in keys)
            {
            }
        }

        private static void RunGarbageCollection()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }
}