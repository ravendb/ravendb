//-----------------------------------------------------------------------
// <copyright file="ToStringTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Testing the ToString methods of the basic types.
    /// </summary>
    [TestClass]
    public class ToStringTests
    {
        /// <summary>
        /// Test JET_INSTANCE.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_INSTANCE.ToString()")]
        public void JetInstanceToString()
        {
            var instance = new JET_INSTANCE() { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_INSTANCE(0x123abc)", instance.ToString());
        }

        /// <summary>
        /// Test JET_SESID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_SESID.ToString()")]
        public void JetSesidToString()
        {
            var sesid = new JET_SESID() { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_SESID(0x123abc)", sesid.ToString());
        }

        /// <summary>
        /// Test JET_DBID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_DBID.ToString()")]
        public void JetDbidToString()
        {
            var dbid = new JET_DBID() { Value = 23 };
            Assert.AreEqual("JET_DBID(23)", dbid.ToString());
        }

        /// <summary>
        /// Test JET_TABLEID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_TABLEID.ToString()")]
        public void JetTableidToString()
        {
            var tableid = new JET_TABLEID() { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_TABLEID(0x123abc)", tableid.ToString());
        }

        /// <summary>
        /// Test JET_COLUMNID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_COLUMNID.ToString()")]
        public void JetColumnidToString()
        {
            var columnid = new JET_COLUMNID() { Value = 0x12EC };
            Assert.AreEqual("JET_COLUMNID(0x12ec)", columnid.ToString());
        }

        /// <summary>
        /// Test JET_OSSNAPID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_OSSNAPID.ToString()")]
        public void JetOsSnapidToString()
        {
            var ossnapid = new JET_OSSNAPID { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_OSSNAPID(0x123abc)", ossnapid.ToString());
        }

        /// <summary>
        /// Test JET_HANDLE.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_HANDLE.ToString()")]
        public void JetHandleToString()
        {
            var handle = new JET_HANDLE { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_HANDLE(0x123abc)", handle.ToString());
        }

        /// <summary>
        /// Test JET_LS.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_LS.ToString()")]
        public void JetLsToString()
        {
            var handle = new JET_LS { Value = (IntPtr)0x123ABC };
            Assert.AreEqual("JET_LS(0x123abc)", handle.ToString());
        }

        /// <summary>
        /// Test JET_INDEXID.ToString().
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_INDEXID.ToString()")]
        public void JetIndexIdToString()
        {
            var indexid = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x2, IndexId3 = 0x3 };
            Assert.AreEqual("JET_INDEXID(0x1:0x2:0x3)", indexid.ToString());
        }
    }
}
