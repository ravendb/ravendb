//-----------------------------------------------------------------------
// <copyright file="CompactDatabaseTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for <see cref="Api.JetCompact"/>.
    /// </summary>
    [TestClass]
    public class CompactDatabaseTests
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
        /// Test <see cref="Api.JetCompact"/>.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetCompact")]
        public void TestJetCompact()
        {
            var test = new DatabaseFileTestHelper("database", true);
            test.TestCompactDatabase();
        }

        /// <summary>
        /// Test <see cref="Api.JetCompact"/> exception handling.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetCompact exception handling")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestJetCompactExceptionHandling()
        {
            var ex = new ArgumentNullException();
            var test = new DatabaseFileTestHelper("database", true);
            test.TestCompactDatabaseCallbackExceptionHandling(ex);
        }
    }
}