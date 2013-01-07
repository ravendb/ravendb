//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnsPerfTest.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Basic performance tests for retrieve columns.
    /// </summary>
    [TestClass]
    public class RetrieveColumnsPerfTest
    {
        /// <summary>
        /// How many times to retrieve the record data.
        /// </summary>
        private const int NumRetrieves =
#if DEBUG
        1000;
#else
        2000000;
#endif

        /// <summary>
        /// The boolean value in the record.
        /// </summary>
        private readonly bool expectedBool = Any.Boolean;

        /// <summary>
        /// The byte value in the record.
        /// </summary>
        private readonly byte expectedByte = Any.Byte;

        /// <summary>
        /// The short value in the record.
        /// </summary>
        private readonly short expectedInt16 = Any.Int16;

        /// <summary>
        /// The ushort value in the record.
        /// </summary>
        private readonly ushort expectedUInt16 = Any.UInt16;

        /// <summary>
        /// The int value in the record.
        /// </summary>
        private readonly int expectedInt32 = Any.Int32;

        /// <summary>
        /// The uint value in the record.
        /// </summary>
        private readonly uint expectedUInt32 = Any.UInt32;

        /// <summary>
        /// The long value in the record.
        /// </summary>
        private readonly long expectedInt64 = Any.Int64;

        /// <summary>
        /// The ulong value in the record.
        /// </summary>
        private readonly ulong expectedUInt64 = Any.UInt64;

        /// <summary>
        /// The guid value in the record.
        /// </summary>
        private readonly Guid expectedGuid = Any.Guid;

        /// <summary>
        /// The float value in the record.
        /// </summary>
        private readonly float expectedFloat = Any.Float;

        /// <summary>
        /// The double value in the record.
        /// </summary>
        private readonly double expectedDouble = Any.Double;

        /// <summary>
        /// The string value in the record.
        /// </summary>
        private readonly string expectedString = Any.StringOfLength(64);

        /// <summary>
        /// The instance to use.
        /// </summary>
        private Instance instance;

        /// <summary>
        /// The session to use.
        /// </summary>
        private Session session;

        /// <summary>
        /// The table to use.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// A dictionary mapping column names to column values.
        /// </summary>
        private Dictionary<string, JET_COLUMNID> columnidDict;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Fixture setup for RetrieveColumnsPerfTest")]
        public void Setup()
        {
            this.instance = new Instance(Guid.NewGuid().ToString(), "RetrieveColumnsPerfTest");
            this.instance.Parameters.NoInformationEvent = true;
            this.instance.Parameters.Recovery = false;
            this.instance.Init();

            this.session = new Session(this.instance);

            // turn off logging so initialization is faster
            this.columnidDict = SetupHelper.CreateTempTableWithAllColumns(this.session, TempTableGrbit.ForceMaterialization, out this.tableid);

            // Insert a record and position the tableid on it
            using (var transaction = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnidDict["boolean"], this.expectedBool);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["byte"], this.expectedByte);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["int16"], this.expectedInt16);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["uint16"], this.expectedUInt16);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["int32"], this.expectedInt32);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["uint32"], this.expectedUInt32);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["int64"], this.expectedInt64);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["uint64"], this.expectedUInt64);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["guid"], this.expectedGuid);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["float"], this.expectedFloat);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["double"], this.expectedDouble);
                Api.SetColumn(this.session, this.tableid, this.columnidDict["unicode"], this.expectedString, Encoding.Unicode);

                update.Save();
                transaction.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            Thread.BeginThreadAffinity();
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Fixture cleanup for RetrieveColumnsPerfTest")]
        public void Teardown()
        {
            Thread.EndThreadAffinity();
            Thread.CurrentThread.Priority = ThreadPriority.Normal;
            Api.JetCloseTable(this.session, this.tableid);
            this.session.End();
            this.instance.Term();
        }

        #endregion

        /// <summary>
        /// Measure performance of reading bookmarks with JetGetBookmark.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of JetGetBookmark")]
        [Priority(3)]
        public void TestJetGetBookmarkPerf()
        {
            DoTest(this.JetGetBookmark, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading bookmarks with GetBookmark.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of GetBookmark")]
        [Priority(3)]
        public void TestGetBookmarkPerf()
        {
            DoTest(this.GetBookmark, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading bookmarks with JetRetrieveKey.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of JetRetrieveKey")]
        [Priority(3)]
        public void TestJetRetrieveKeyPerf()
        {
            DoTest(this.JetRetrieveKey, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading bookmarks with RetrieveKey.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveKey")]
        [Priority(3)]
        public void TestRetrieveKeyPerf()
        {
            DoTest(this.RetrieveKey, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with JetRetrieveColumns.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of JetRetrieveColumns")]
        [Priority(3)]
        public void TestJetRetrieveColumnsPerf()
        {
            DoTest(this.RetrieveWithJetRetrieveColumns, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with JetRetrieveColumns, using one buffer to retrieve all data.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of JetRetrieveColumns, using one buffer to retrieve all data")]
        [Priority(3)]
        public void TestJetRetrieveColumnsOneBufferPerf()
        {
            DoTest(this.RetrieveWithJetRetrieveColumnsOneBuffer, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumn, which allocates memory for every column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumn")]
        [Priority(3)]
        public void TestRetrieveColumnPerf()
        {
            DoTest(this.RetrieveWithRetrieveColumn, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumnAs.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAs")]
        [Priority(3)]
        public void TestRetrieveColumnAsPerf()
        {
            DoTest(this.RetrieveWithRetrieveColumnAs, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumns.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumns")]
        [Priority(3)]
        public void TestRetrieveColumnsPerf()
        {
            DoTest(this.RetrieveWithRetrieveColumns, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumnsAsObject.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnsAsObject")]
        [Priority(3)]
        public void TestRetrieveColumnsAsObjectPerf()
        {
            DoTest(this.RetrieveWithRetrieveColumnsAsObject, 8 * 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumns and lots of allocations.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumns (slow)")]
        [Priority(3)]
        public void TestRetrieveColumnsSlowPerf()
        {
            DoTest(this.RetrieveWithRetrieveColumnsSlow, 1024 * 1024);
        }

        /// <summary>
        /// Measure performance of reading records with RetrieveColumnAsString.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString")]
        [Priority(3)]
        public void TestJetRetrieveColumnPerf()
        {
            DoTest(this.RetrieveWithJetRetrieveColumn, 1024 * 1024);
        }

        #region Helper Methods

        /// <summary>
        /// Perform an action, timing it and checking the system's memory usage before and after.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        /// <param name="memoryLeakThreshold">
        /// Number of allocated bytes that will trigger memory leak detection.
        /// </param>
        private static void DoTest(Action action, int memoryLeakThreshold)
        {
            RunGarbageCollection();

            long memoryAtStart = GC.GetTotalMemory(true);
            int collectionCountAtStart = GC.CollectionCount(0);

            TimeAction(action);

            int collectionCountAtEnd = GC.CollectionCount(0);
            RunGarbageCollection();
            long memoryAtEnd = GC.GetTotalMemory(true);
            long memoryDelta = memoryAtEnd - memoryAtStart;
            Console.WriteLine(
                "Memory changed by {0} bytes ({1} GC cycles)",
                memoryDelta,
                collectionCountAtEnd - collectionCountAtStart);
            Assert.IsTrue(memoryDelta < memoryLeakThreshold, "Too much memory used. Memory leak?");
        }

        /// <summary>
        /// Perform and time the given action.
        /// </summary>
        /// <param name="action">The operation to perform.</param>
        private static void TimeAction(Action action)
        {
            // Let other threads run before we start our test
            Thread.Sleep(1);
            var stopwatch = EsentStopwatch.StartNew();
            action();
            stopwatch.Stop();
            Console.WriteLine("{0} ({1})", stopwatch.Elapsed, stopwatch.ThreadStats);
        }

        /// <summary>
        /// Run garbage collection.
        /// </summary>
        private static void RunGarbageCollection()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// Returns a Guid value converted from 16 bytes at a specified position in a byte array.
        /// </summary>
        /// <param name="value">
        /// The array containing the data to convert..
        /// </param>
        /// <param name="startIndex">
        /// The index in the data to start converting from.
        /// </param>
        /// <returns>
        /// A guid converted from the data;
        /// </returns>
        private static unsafe Guid ToGuid(byte[] value, int startIndex)
        {
            Guid guid = Guid.Empty;
            byte* dest = (byte*)&guid;
            for (int j = 0; j < 16; j += 4)
            {
                dest[j + 0] = value[startIndex + j + 0];
                dest[j + 1] = value[startIndex + j + 1];
                dest[j + 2] = value[startIndex + j + 2];
                dest[j + 3] = value[startIndex + j + 3];
            }

            return guid;
        }

        /// <summary>
        /// Retrieve columns using the JetGetBookmark API.
        /// </summary>
        private void JetGetBookmark()
        {
            byte[] bookmark = new byte[SystemParameters.BookmarkMost];
            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                int bookmarkSize;
                Api.JetGetBookmark(this.session, this.tableid, bookmark, bookmark.Length, out bookmarkSize);
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the GetBookmark API.
        /// </summary>
        private void GetBookmark()
        {
            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                byte[] bookmark = Api.GetBookmark(this.session, this.tableid);
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the JetRetrieveKey API.
        /// </summary>
        private void JetRetrieveKey()
        {
            byte[] key = new byte[SystemParameters.KeyMost];
            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                int keySize;
                Api.JetRetrieveKey(this.session, this.tableid, key, key.Length, out keySize, RetrieveKeyGrbit.None);
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the RetrieveKey API.
        /// </summary>
        private void RetrieveKey()
        {
            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                byte[] key = Api.RetrieveKey(this.session, this.tableid, RetrieveKeyGrbit.None);
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic JetRetrieveColumn API.
        /// </summary>
        private void RetrieveWithJetRetrieveColumn()
        {
            var boolBuffer = new byte[sizeof(bool)];
            var byteBuffer = new byte[sizeof(byte)];
            var int16Buffer = new byte[sizeof(short)];
            var uint16Buffer = new byte[sizeof(ushort)];
            var int32Buffer = new byte[sizeof(int)];
            var uint32Buffer = new byte[sizeof(uint)];
            var int64Buffer = new byte[sizeof(long)];
            var uint64Buffer = new byte[sizeof(ulong)];
            var guidBuffer = new byte[16];
            var floatBuffer = new byte[sizeof(float)];
            var doubleBuffer = new byte[sizeof(double)];
            var stringBuffer = new byte[512];

            JET_COLUMNID boolColumn = this.columnidDict["boolean"];
            JET_COLUMNID byteColumn = this.columnidDict["byte"];
            JET_COLUMNID int16Column = this.columnidDict["int16"];
            JET_COLUMNID uint16Column = this.columnidDict["uint16"];
            JET_COLUMNID int32Column = this.columnidDict["int32"];
            JET_COLUMNID uint32Column = this.columnidDict["uint32"];
            JET_COLUMNID int64Column = this.columnidDict["int64"];
            JET_COLUMNID uint64Column = this.columnidDict["uint64"];
            JET_COLUMNID guidColumn = this.columnidDict["guid"];
            JET_COLUMNID floatColumn = this.columnidDict["float"];
            JET_COLUMNID doubleColumn = this.columnidDict["double"];
            JET_COLUMNID stringColumn = this.columnidDict["unicode"];

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {                
                int actualSize;
                Api.JetRetrieveColumn(this.session, this.tableid, boolColumn, boolBuffer, boolBuffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, byteColumn, byteBuffer, byteBuffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, int16Column, int16Buffer, int16Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, uint16Column, uint16Buffer, uint16Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, int32Column, int32Buffer, int32Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, uint32Column, uint32Buffer, uint32Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, int64Column, int64Buffer, int64Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, uint64Column, uint64Buffer, uint64Buffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, guidColumn, guidBuffer, guidBuffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, floatColumn, floatBuffer, floatBuffer.Length, out actualSize, RetrieveColumnGrbit.None, null);
                Api.JetRetrieveColumn(this.session, this.tableid, doubleColumn, doubleBuffer, doubleBuffer.Length, out actualSize, RetrieveColumnGrbit.None, null);

                int stringSize;
                Api.JetRetrieveColumn(this.session, this.tableid, stringColumn, stringBuffer, stringBuffer.Length, out stringSize, RetrieveColumnGrbit.None, null);

                bool actualBool = BitConverter.ToBoolean(boolBuffer, 0);
                byte actualByte = byteBuffer[0];
                short actualInt16 = BitConverter.ToInt16(int16Buffer, 0);
                ushort actualUInt16 = BitConverter.ToUInt16(uint16Buffer, 0);
                int actualInt32 = BitConverter.ToInt32(int32Buffer, 0);
                uint actualUInt32 = BitConverter.ToUInt32(uint32Buffer, 0);
                long actualInt64 = BitConverter.ToInt64(int64Buffer, 0);
                ulong actualUInt64 = BitConverter.ToUInt64(uint64Buffer, 0);
                Guid actualGuid = new Guid(guidBuffer);
                float actualFloat = BitConverter.ToSingle(floatBuffer, 0);
                double actualDouble = BitConverter.ToDouble(doubleBuffer, 0);
                string actualString = Encoding.Unicode.GetString(stringBuffer, 0, stringSize);

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic JetRetrieveColumns API.
        /// </summary>
        private void RetrieveWithJetRetrieveColumns()
        {
            var boolBuffer = new byte[sizeof(bool)];
            var byteBuffer = new byte[sizeof(byte)];
            var int16Buffer = new byte[sizeof(short)];
            var uint16Buffer = new byte[sizeof(ushort)];
            var int32Buffer = new byte[sizeof(int)];
            var uint32Buffer = new byte[sizeof(uint)];
            var int64Buffer = new byte[sizeof(long)];
            var uint64Buffer = new byte[sizeof(ulong)];
            var guidBuffer = new byte[16];
            var floatBuffer = new byte[sizeof(float)];
            var doubleBuffer = new byte[sizeof(double)];
            var stringBuffer = new byte[512];

            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["boolean"], pvData = boolBuffer, cbData = boolBuffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["byte"], pvData = byteBuffer, cbData = byteBuffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int16"], pvData = int16Buffer, cbData = int16Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint16"], pvData = uint16Buffer, cbData = uint16Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int32"], pvData = int32Buffer, cbData = int32Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint32"], pvData = uint32Buffer, cbData = uint32Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int64"], pvData = int64Buffer, cbData = int64Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint64"], pvData = uint64Buffer, cbData = uint64Buffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["guid"], pvData = guidBuffer, cbData = guidBuffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["float"], pvData = floatBuffer, cbData = floatBuffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["double"], pvData = doubleBuffer, cbData = doubleBuffer.Length, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["unicode"], pvData = stringBuffer, cbData = stringBuffer.Length, itagSequence = 1 },
            };

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                Api.JetRetrieveColumns(this.session, this.tableid, retrievecolumns, retrievecolumns.Length);

                bool actualBool = BitConverter.ToBoolean(boolBuffer, 0);
                byte actualByte = byteBuffer[0];
                short actualInt16 = BitConverter.ToInt16(int16Buffer, 0);
                ushort actualUInt16 = BitConverter.ToUInt16(uint16Buffer, 0);
                int actualInt32 = BitConverter.ToInt32(int32Buffer, 0);
                uint actualUInt32 = BitConverter.ToUInt32(uint32Buffer, 0);
                long actualInt64 = BitConverter.ToInt64(int64Buffer, 0);
                ulong actualUInt64 = BitConverter.ToUInt64(uint64Buffer, 0);
                Guid actualGuid = new Guid(guidBuffer);
                float actualFloat = BitConverter.ToSingle(floatBuffer, 0);
                double actualDouble = BitConverter.ToDouble(doubleBuffer, 0);
                string actualString = Encoding.Unicode.GetString(stringBuffer, 0, retrievecolumns[11].cbActual);

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic JetRetrieveColumns API, using one buffer to retrieve all data.
        /// </summary>
        private void RetrieveWithJetRetrieveColumnsOneBuffer()
        {
            // Calculate the buffer size and allocate it
            const int BufferSize = 1024;
            var buffer = new byte[BufferSize];

            // Create the JET_RETRIEVECOLUMN array without any buffers
            // The boolean and byte columns are retrieved last because their size is 1 and we don't want the other members
            // to be located at odd offsets (that makes conversion slower).
            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int16"], cbData = sizeof(short), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint16"], cbData = sizeof(ushort), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int32"], cbData = sizeof(int), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint32"], cbData = sizeof(uint), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["int64"], cbData = sizeof(long), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["uint64"], cbData = sizeof(ulong), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["guid"], cbData = 16, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["double"], cbData = sizeof(double), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["float"], cbData = sizeof(float), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["unicode"], cbData = 512, itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["boolean"], cbData = sizeof(bool), itagSequence = 1 },
                new JET_RETRIEVECOLUMN { columnid = this.columnidDict["byte"], cbData = sizeof(byte), itagSequence = 1 },
            };

            // Set the pvData/ibData members of the JET_RETRIEVECOLUMN objects
            int offset = 0;
            foreach (JET_RETRIEVECOLUMN retrievecolumn in retrievecolumns)
            {
                retrievecolumn.pvData = buffer;
                retrievecolumn.ibData = offset;
                offset += retrievecolumn.cbData;
            }

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                Api.JetRetrieveColumns(this.session, this.tableid, retrievecolumns, retrievecolumns.Length);

                short actualInt16 = BitConverter.ToInt16(retrievecolumns[0].pvData, retrievecolumns[0].ibData);
                ushort actualUInt16 = BitConverter.ToUInt16(retrievecolumns[1].pvData, retrievecolumns[1].ibData);
                int actualInt32 = BitConverter.ToInt32(retrievecolumns[2].pvData, retrievecolumns[2].ibData);
                uint actualUInt32 = BitConverter.ToUInt32(retrievecolumns[3].pvData, retrievecolumns[3].ibData);
                long actualInt64 = BitConverter.ToInt64(retrievecolumns[4].pvData, retrievecolumns[4].ibData);
                ulong actualUInt64 = BitConverter.ToUInt64(retrievecolumns[5].pvData, retrievecolumns[5].ibData);
                Guid actualGuid = ToGuid(retrievecolumns[6].pvData, retrievecolumns[6].ibData);
                double actualDouble = BitConverter.ToDouble(retrievecolumns[7].pvData, retrievecolumns[7].ibData);
                float actualFloat = BitConverter.ToSingle(retrievecolumns[8].pvData, retrievecolumns[8].ibData);
                string actualString = Encoding.Unicode.GetString(retrievecolumns[9].pvData, retrievecolumns[9].ibData, retrievecolumns[9].cbActual);
                bool actualBool = BitConverter.ToBoolean(retrievecolumns[10].pvData, retrievecolumns[10].ibData);
                byte actualByte = retrievecolumns[11].pvData[retrievecolumns[11].ibData];

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic RetrieveColumn API.
        /// </summary>
        private void RetrieveWithRetrieveColumn()
        {
            JET_COLUMNID boolColumn = this.columnidDict["boolean"];
            JET_COLUMNID byteColumn = this.columnidDict["byte"];
            JET_COLUMNID int16Column = this.columnidDict["int16"];
            JET_COLUMNID uint16Column = this.columnidDict["uint16"];
            JET_COLUMNID int32Column = this.columnidDict["int32"];
            JET_COLUMNID uint32Column = this.columnidDict["uint32"];
            JET_COLUMNID int64Column = this.columnidDict["int64"];
            JET_COLUMNID uint64Column = this.columnidDict["uint64"];
            JET_COLUMNID guidColumn = this.columnidDict["guid"];
            JET_COLUMNID floatColumn = this.columnidDict["float"];
            JET_COLUMNID doubleColumn = this.columnidDict["double"];
            JET_COLUMNID stringColumn = this.columnidDict["unicode"];

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                byte[] boolBuffer = Api.RetrieveColumn(this.session, this.tableid, boolColumn);
                byte[] byteBuffer = Api.RetrieveColumn(this.session, this.tableid, byteColumn);
                byte[] int16Buffer = Api.RetrieveColumn(this.session, this.tableid, int16Column);
                byte[] uint16Buffer = Api.RetrieveColumn(this.session, this.tableid, uint16Column);
                byte[] int32Buffer = Api.RetrieveColumn(this.session, this.tableid, int32Column);
                byte[] uint32Buffer = Api.RetrieveColumn(this.session, this.tableid, uint32Column);
                byte[] int64Buffer = Api.RetrieveColumn(this.session, this.tableid, int64Column);
                byte[] uint64Buffer = Api.RetrieveColumn(this.session, this.tableid, uint64Column);
                byte[] guidBuffer = Api.RetrieveColumn(this.session, this.tableid, guidColumn);
                byte[] floatBuffer = Api.RetrieveColumn(this.session, this.tableid, floatColumn);
                byte[] doubleBuffer = Api.RetrieveColumn(this.session, this.tableid, doubleColumn);
                byte[] stringBuffer = Api.RetrieveColumn(this.session, this.tableid, stringColumn);

                bool actualBool = BitConverter.ToBoolean(boolBuffer, 0);
                byte actualByte = byteBuffer[0];
                short actualInt16 = BitConverter.ToInt16(int16Buffer, 0);
                ushort actualUInt16 = BitConverter.ToUInt16(uint16Buffer, 0);
                int actualInt32 = BitConverter.ToInt32(int32Buffer, 0);
                uint actualUInt32 = BitConverter.ToUInt32(uint32Buffer, 0);
                long actualInt64 = BitConverter.ToInt64(int64Buffer, 0);
                ulong actualUInt64 = BitConverter.ToUInt64(uint64Buffer, 0);
                Guid actualGuid = new Guid(guidBuffer);
                float actualFloat = BitConverter.ToSingle(floatBuffer, 0);
                double actualDouble = BitConverter.ToDouble(doubleBuffer, 0);
                string actualString = Encoding.Unicode.GetString(stringBuffer, 0, stringBuffer.Length);

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the RetrieveColumnAs APIs.
        /// </summary>
        private void RetrieveWithRetrieveColumnAs()
        {
            JET_COLUMNID boolColumn = this.columnidDict["boolean"];
            JET_COLUMNID byteColumn = this.columnidDict["byte"];
            JET_COLUMNID int16Column = this.columnidDict["int16"];
            JET_COLUMNID uint16Column = this.columnidDict["uint16"];
            JET_COLUMNID int32Column = this.columnidDict["int32"];
            JET_COLUMNID uint32Column = this.columnidDict["uint32"];
            JET_COLUMNID int64Column = this.columnidDict["int64"];
            JET_COLUMNID uint64Column = this.columnidDict["uint64"];
            JET_COLUMNID guidColumn = this.columnidDict["guid"];
            JET_COLUMNID floatColumn = this.columnidDict["float"];
            JET_COLUMNID doubleColumn = this.columnidDict["double"];
            JET_COLUMNID stringColumn = this.columnidDict["unicode"];

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                bool actualBool = (bool)Api.RetrieveColumnAsBoolean(this.session, this.tableid, boolColumn);
                byte actualByte = (byte)Api.RetrieveColumnAsByte(this.session, this.tableid, byteColumn);
                short actualInt16 = (short)Api.RetrieveColumnAsInt16(this.session, this.tableid, int16Column);
                ushort actualUInt16 = (ushort)Api.RetrieveColumnAsUInt16(this.session, this.tableid, uint16Column);
                int actualInt32 = (int)Api.RetrieveColumnAsInt32(this.session, this.tableid, int32Column);
                uint actualUInt32 = (uint)Api.RetrieveColumnAsUInt32(this.session, this.tableid, uint32Column);
                long actualInt64 = (long)Api.RetrieveColumnAsInt64(this.session, this.tableid, int64Column);
                ulong actualUInt64 = (ulong)Api.RetrieveColumnAsUInt64(this.session, this.tableid, uint64Column);
                Guid actualGuid = (Guid)Api.RetrieveColumnAsGuid(this.session, this.tableid, guidColumn);
                float actualFloat = (float)Api.RetrieveColumnAsFloat(this.session, this.tableid, floatColumn);
                double actualDouble = (double)Api.RetrieveColumnAsDouble(this.session, this.tableid, doubleColumn);
                string actualString = Api.RetrieveColumnAsString(this.session, this.tableid, stringColumn);

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic RetrieveColumns API.
        /// </summary>
        private void RetrieveWithRetrieveColumns()
        {
            var boolColumn = new BoolColumnValue { Columnid = this.columnidDict["boolean"] };
            var byteColumn = new ByteColumnValue { Columnid = this.columnidDict["byte"] };
            var int16Column = new Int16ColumnValue { Columnid = this.columnidDict["int16"] };
            var uint16Column = new UInt16ColumnValue { Columnid = this.columnidDict["uint16"] };
            var int32Column = new Int32ColumnValue { Columnid = this.columnidDict["int32"] };
            var uint32Column = new UInt32ColumnValue { Columnid = this.columnidDict["uint32"] };
            var int64Column = new Int64ColumnValue { Columnid = this.columnidDict["int64"] };
            var uint64Column = new UInt64ColumnValue { Columnid = this.columnidDict["uint64"] };
            var guidColumn = new GuidColumnValue { Columnid = this.columnidDict["guid"] };
            var floatColumn = new FloatColumnValue { Columnid = this.columnidDict["float"] };
            var doubleColumn = new DoubleColumnValue { Columnid = this.columnidDict["double"] };
            var stringColumn = new StringColumnValue { Columnid = this.columnidDict["unicode"] };

            var retrievecolumns = new ColumnValue[]
            {
                boolColumn,
                byteColumn,
                int16Column,
                uint16Column,
                int32Column,
                uint32Column,
                int64Column,
                uint64Column,
                guidColumn,
                floatColumn,
                doubleColumn,
                stringColumn,
            };

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                Api.RetrieveColumns(this.session, this.tableid, retrievecolumns);

                bool actualBool = (bool)boolColumn.Value;
                byte actualByte = (byte)byteColumn.Value;
                short actualInt16 = (short)int16Column.Value;
                ushort actualUInt16 = (ushort)uint16Column.Value;
                int actualInt32 = (int)int32Column.Value;
                uint actualUInt32 = (uint)uint32Column.Value;
                long actualInt64 = (long)int64Column.Value;
                ulong actualUInt64 = (ulong)uint64Column.Value;
                Guid actualGuid = (Guid)guidColumn.Value;
                float actualFloat = (float)floatColumn.Value;
                double actualDouble = (double)doubleColumn.Value;
                string actualString = stringColumn.Value;

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic RetrieveColumns API.
        /// </summary>
        private void RetrieveWithRetrieveColumnsAsObject()
        {
            var boolColumn = new BoolColumnValue { Columnid = this.columnidDict["boolean"] };
            var byteColumn = new ByteColumnValue { Columnid = this.columnidDict["byte"] };
            var int16Column = new Int16ColumnValue { Columnid = this.columnidDict["int16"] };
            var uint16Column = new UInt16ColumnValue { Columnid = this.columnidDict["uint16"] };
            var int32Column = new Int32ColumnValue { Columnid = this.columnidDict["int32"] };
            var uint32Column = new UInt32ColumnValue { Columnid = this.columnidDict["uint32"] };
            var int64Column = new Int64ColumnValue { Columnid = this.columnidDict["int64"] };
            var uint64Column = new UInt64ColumnValue { Columnid = this.columnidDict["uint64"] };
            var guidColumn = new GuidColumnValue { Columnid = this.columnidDict["guid"] };
            var floatColumn = new FloatColumnValue { Columnid = this.columnidDict["float"] };
            var doubleColumn = new DoubleColumnValue { Columnid = this.columnidDict["double"] };
            var stringColumn = new StringColumnValue { Columnid = this.columnidDict["unicode"] };

            var retrievecolumns = new ColumnValue[]
            {
                boolColumn,
                byteColumn,
                int16Column,
                uint16Column,
                int32Column,
                uint32Column,
                int64Column,
                uint64Column,
                guidColumn,
                floatColumn,
                doubleColumn,
                stringColumn,
            };

            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                Api.RetrieveColumns(this.session, this.tableid, retrievecolumns);

                bool actualBool = (bool)boolColumn.ValueAsObject;
                byte actualByte = (byte)byteColumn.ValueAsObject;
                short actualInt16 = (short)int16Column.ValueAsObject;
                ushort actualUInt16 = (ushort)uint16Column.ValueAsObject;
                int actualInt32 = (int)int32Column.ValueAsObject;
                uint actualUInt32 = (uint)uint32Column.ValueAsObject;
                long actualInt64 = (long)int64Column.ValueAsObject;
                ulong actualUInt64 = (ulong)uint64Column.ValueAsObject;
                Guid actualGuid = (Guid)guidColumn.ValueAsObject;
                float actualFloat = (float)floatColumn.ValueAsObject;
                double actualDouble = (double)doubleColumn.ValueAsObject;
                string actualString = stringColumn.Value;

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Retrieve columns using the basic RetrieveColumns API, but with the
        /// ColumnValue objects allocated inside of the loop.
        /// </summary>
        private void RetrieveWithRetrieveColumnsSlow()
        {
            Api.JetBeginTransaction(this.session);
            for (int i = 0; i < NumRetrieves; ++i)
            {
                var boolColumn = new BoolColumnValue { Columnid = this.columnidDict["boolean"] };
                var byteColumn = new ByteColumnValue { Columnid = this.columnidDict["byte"] };
                var int16Column = new Int16ColumnValue { Columnid = this.columnidDict["int16"] };
                var uint16Column = new UInt16ColumnValue { Columnid = this.columnidDict["uint16"] };
                var int32Column = new Int32ColumnValue { Columnid = this.columnidDict["int32"] };
                var uint32Column = new UInt32ColumnValue { Columnid = this.columnidDict["uint32"] };
                var int64Column = new Int64ColumnValue { Columnid = this.columnidDict["int64"] };
                var uint64Column = new UInt64ColumnValue { Columnid = this.columnidDict["uint64"] };
                var guidColumn = new GuidColumnValue { Columnid = this.columnidDict["guid"] };
                var floatColumn = new FloatColumnValue { Columnid = this.columnidDict["float"] };
                var doubleColumn = new DoubleColumnValue { Columnid = this.columnidDict["double"] };
                var stringColumn = new StringColumnValue { Columnid = this.columnidDict["unicode"] };

                Api.RetrieveColumns(
                    this.session,
                    this.tableid,
                    boolColumn,
                    byteColumn,
                    int16Column,
                    uint16Column,
                    int32Column,
                    uint32Column,
                    int64Column,
                    uint64Column,
                    guidColumn,
                    floatColumn,
                    doubleColumn,
                    stringColumn);

                bool actualBool = (bool)boolColumn.Value;
                byte actualByte = (byte)byteColumn.Value;
                short actualInt16 = (short)int16Column.Value;
                ushort actualUInt16 = (ushort)uint16Column.Value;
                int actualInt32 = (int)int32Column.Value;
                uint actualUInt32 = (uint)uint32Column.Value;
                long actualInt64 = (long)int64Column.Value;
                ulong actualUInt64 = (ulong)uint64Column.Value;
                Guid actualGuid = (Guid)guidColumn.Value;
                float actualFloat = (float)floatColumn.Value;
                double actualDouble = (double)doubleColumn.Value;
                string actualString = stringColumn.Value;

#if DEBUG
                Assert.AreEqual(this.expectedBool, actualBool, "boolean");
                Assert.AreEqual(this.expectedByte, actualByte, "byte");
                Assert.AreEqual(this.expectedInt16, actualInt16, "int16");
                Assert.AreEqual(this.expectedUInt16, actualUInt16, "uint16");
                Assert.AreEqual(this.expectedInt32, actualInt32, "int32");
                Assert.AreEqual(this.expectedUInt32, actualUInt32, "uint32");
                Assert.AreEqual(this.expectedInt64, actualInt64, "int64");
                Assert.AreEqual(this.expectedUInt64, actualUInt64, "uint64");
                Assert.AreEqual(this.expectedGuid, actualGuid, "guid");
                Assert.AreEqual(this.expectedFloat, actualFloat, "float");
                Assert.AreEqual(this.expectedDouble, actualDouble, "double");
                Assert.AreEqual(this.expectedString, actualString, "unicode");
#endif
            }

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }

        #endregion
    }
}
