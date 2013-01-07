//-----------------------------------------------------------------------
// <copyright file="RecordlistConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the JET_RECORDLIST creation from NATIVE_RECORDLIST.
    /// </summary>
    [TestClass]
    public class RecordlistConversionsTests
    {       
        /// <summary>
        /// The native recordlist that will be converted into a managed object.
        /// </summary>
        private NATIVE_RECORDLIST native;

        /// <summary>
        /// The managed version of the native recordlist.
        /// </summary>
        private JET_RECORDLIST converted;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the RecordlistConversionsTests test fixture")]
        public void Setup()
        {
            this.native = new NATIVE_RECORDLIST
            {
                tableid = (IntPtr)0x1000,
                cRecords = 100,
                columnidBookmark = 1,
            };

            this.converted = new JET_RECORDLIST();
            this.converted.SetFromNativeRecordlist(this.native);
        }

        /// <summary>
        /// Check the conversion of tableid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from NATIVE_RECORDLIST to JET_RECORDLIST sets tableid")]
        public void VerifyConvertRecordlistFromNativeSetsTableid()
        {
            Assert.AreEqual(new JET_TABLEID { Value = this.native.tableid }, this.converted.tableid);
        }

        /// <summary>
        /// Check the conversion of cRecord.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from NATIVE_RECORDLIST to JET_RECORDLIST sets cRecord")]
        public void VerifyConvertRecordlistFromNativeSetsCrecords()
        {
            Assert.AreEqual((int)this.native.cRecords, this.converted.cRecords);
        }

        /// <summary>
        /// Check the conversion of columnidBookmark.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from NATIVE_RECORDLIST to JET_RECORDLIST sets columnidBookmark")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidBookmark()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidBookmark }, this.converted.columnidBookmark);
        }
    }
}