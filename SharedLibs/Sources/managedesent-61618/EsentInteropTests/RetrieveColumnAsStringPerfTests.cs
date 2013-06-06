//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnAsStringPerfTests.cs" company="Microsoft Corporation">
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
    public class RetrieveColumnAsStringPerfTests
    {
        /// <summary>
        /// How many times to retrieve the record data.
        /// </summary>
        private const int NumRetrieves = 
#if DEBUG
        1000;
#else
        5000000;
#endif

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
        [Description("Fixture setup for RetrieveColumnAsStringPerfTests")]
        public void Setup()
        {
            this.instance = new Instance(Guid.NewGuid().ToString(), "RetrieveColumnAsStringPerfTests");
            this.instance.Parameters.NoInformationEvent = true;
            this.instance.Parameters.Recovery = false;
            this.instance.Init();

            this.session = new Session(this.instance);

            // turn off logging so initialization is faster
            this.columnidDict = SetupHelper.CreateTempTableWithAllColumns(this.session, TempTableGrbit.ForceMaterialization, out this.tableid);

            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            Thread.BeginThreadAffinity();
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Fixture cleanup for RetrieveColumnAsStringPerfTests")]
        public void Teardown()
        {
            Thread.EndThreadAffinity();
            Thread.CurrentThread.Priority = ThreadPriority.Normal;
            Api.JetCloseTable(this.session, this.tableid);
            this.session.End();
            this.instance.Term();
        }

        #endregion

        #region Tests

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a short ASCII column
        /// and a custom encoder.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a short ASCII column and a custom encoder")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringShortCustomAscii()
        {
            this.TimeRetrieveColumnAsString("ascii", 8, new ASCIIEncoding());
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a short ASCII column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a short ASCII column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringShortAscii()
        {
            this.TimeRetrieveColumnAsString("ascii", 8, Encoding.ASCII);
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with an ASCII column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with an ASCII column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringAscii()
        {
            this.TimeRetrieveColumnAsString("ascii", 512, Encoding.ASCII);
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a long ASCII column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a long ASCII column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringLongAscii()
        {
            this.TimeRetrieveColumnAsString("ascii", 513, Encoding.ASCII);
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a short Unicode column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a short Unicode column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringShortUnicode()
        {
            this.TimeRetrieveColumnAsString("unicode", 8, Encoding.Unicode);
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a Unicode column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a Unicode column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringUnicode()
        {
            this.TimeRetrieveColumnAsString("unicode", 512, Encoding.Unicode);
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a Unicode column
        /// and a custom encoding.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a Unicode column and a custom encoding")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringUnicodeCustomEncoding()
        {
            this.TimeRetrieveColumnAsString("unicode", 512, new UnicodeEncoding(false, false, true));
        }

        /// <summary>
        /// Test the performance of RetrieveColumnAsString with a long Unicode column.
        /// </summary>
        [TestMethod]
        [Description("Test the performance of RetrieveColumnAsString with a long Unicode column")]
        [Priority(3)]
        public void TimeRetrieveColumnAsStringLongUnicode()
        {
            this.TimeRetrieveColumnAsString("unicode", 513, Encoding.Unicode);
        }

        #endregion

        /// <summary>
        /// Retrieve columns using the RetrieveColumnAsString API.
        /// </summary>
        /// <param name="columnName">Name of the column to set.</param>
        /// <param name="numChars">Number of characters in the string to set.</param>
        /// <param name="encoding">The encoding to use.</param>
        private void TimeRetrieveColumnAsString(string columnName, int numChars, Encoding encoding)
        {
            string expected = Any.StringOfLength(numChars);

            // Insert a record and position the tableid on it.
            JET_COLUMNID columnid = this.columnidDict[columnName];
            using (var transaction = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, columnid, expected, encoding, SetColumnGrbit.IntrinsicLV);

                update.SaveAndGotoBookmark();
                transaction.Commit(CommitTransactionGrbit.None);
            }

            Api.JetBeginTransaction(this.session);

            // Retrieve the column once to make sure we get the correct value.
            string actual = Api.RetrieveColumnAsString(this.session, this.tableid, columnid, encoding);
            Assert.AreEqual(expected, actual);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Thread.Sleep(1);

            // Time the retrieves
            var stopwatch = EsentStopwatch.StartNew();

            for (int i = 0; i < NumRetrieves; ++i)
            {
                actual = Api.RetrieveColumnAsString(this.session, this.tableid, columnid, encoding);
            }

            stopwatch.Stop();
            Console.WriteLine("{0} ({1})", stopwatch.Elapsed, stopwatch.ThreadStats);

            Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
        }
    }
}