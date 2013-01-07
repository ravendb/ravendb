//-----------------------------------------------------------------------
// <copyright file="JetIdleTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JetIdle
    /// </summary>
    [TestClass]
    public class JetIdleTests
    {
        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the JetIdleTests fixture")]
        public void Setup()
        {
            this.instance = SetupHelper.CreateNewInstance(".");

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the JetIdleTests fixture")]
        public void Teardown()
        {
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test JetIdle with the default grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JetIdle with the default grbit")]
        public void TestDefault()
        {
            Assert.AreEqual(JET_wrn.NoIdleActivity, Api.JetIdle(this.sesid, IdleGrbit.None));
        }

        /// <summary>
        /// Test JetIdle with the compact grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JetIdle with the compact grbit")]
        public void TestCompact()
        {
            Assert.AreEqual(JET_wrn.NoIdleActivity, Api.JetIdle(this.sesid, IdleGrbit.Compact));
        }

        /// <summary>
        /// Test JetIdle with the status grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JetIdle with the status grbit")]
        public void TestGetStatus()
        {
            Assert.AreEqual(JET_wrn.Success, Api.JetIdle(this.sesid, IdleGrbit.GetStatus));
        }
    }
}
