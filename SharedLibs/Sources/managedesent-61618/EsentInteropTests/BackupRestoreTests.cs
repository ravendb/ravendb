//-----------------------------------------------------------------------
// <copyright file="BackupRestoreTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JetBackupInstance and JetRestoreInstance.
    /// </summary>
    [TestClass]
    public partial class BackupRestoreTests
    {
        #region Setup/Teardown
        /// <summary>
        /// Verifies no instances are leaked.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            SetupHelper.CheckProcessForInstanceLeaks();
        }
        #endregion

        /// <summary>
        /// Verify JetRestoreInstance throws an exception when the source database is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetRestoreInstance throws an exception when the source database is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestJetRestoreInstanceThrowsExceptionWhenSourceIsNull()
        {
            using (var instance = new Instance("RestoreNullSource"))
            {
                Api.JetRestoreInstance(instance, null, "somewhere", null);
            }
        }

        /// <summary>
        /// Test exception handling for exceptions thrown from
        /// the status callback during JetBackup.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetRestore without a status callback")]
        public void TestBackupRestoreWithoutStatusCallback()
        {
            var test = new DatabaseFileTestHelper("database", "backup", false);
            test.TestBackupRestore();
        }

        /// <summary>
        /// Test exception handling for exceptions thrown from
        /// the status callback during JetBackup.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test exception handling for exceptions thrown from the status callback during JetBackup")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestBackupCallbackExceptionHandling()
        {
            var ex = new ArgumentNullException();
            var test = new DatabaseFileTestHelper("database", "backup", true);
            test.TestBackupCallbackExceptionHandling(ex);
        }
    }
}
