//-----------------------------------------------------------------------
// <copyright file="IndexlistTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the JET_INDEXLIST class
    /// </summary>
    [TestClass]
    public class IndexlistConversionsTests
    {       
        /// <summary>
        /// The native index list that will be converted into a managed object.
        /// </summary>
        private NATIVE_INDEXLIST native;

        /// <summary>
        /// The managed version of the native indexlist.
        /// </summary>
        private JET_INDEXLIST converted;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the IndexlistConversionsTests fixture")]
        public void Setup()
        {
            this.native = new NATIVE_INDEXLIST()
            {
                tableid = (IntPtr)0x1000,
                cRecord = 100,
                columnidindexname = 0,
                columnidgrbitIndex = 1,
                columnidcKey = 2,
                columnidcEntry = 3,
                columnidcPage = 4,
                columnidcColumn = 5,
                columnidiColumn = 6,
                columnidcolumnid = 7,
                columnidcoltyp = 8,
                columnidCountry = 9,
                columnidLangid = 10,
                columnidCp = 11,
                columnidCollate = 12,
                columnidgrbitColumn = 13,
                columnidcolumnname = 14,
                columnidLCMapFlags = 15,
            };

            this.converted = new JET_INDEXLIST();
            this.converted.SetFromNativeIndexlist(this.native);
        }

        /// <summary>
        /// Check the conversion of tableid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets the tableid")]
        public void VerifyConvertIndexlistFromNativeSetsTableid()
        {
            Assert.AreEqual(new JET_TABLEID { Value = this.native.tableid }, this.converted.tableid);
        }

        /// <summary>
        /// Check the conversion of cRecord.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets cRecord")]
        public void VerifyConvertIndexlistFromNativeSetsCrecord()
        {
            Assert.AreEqual((int)this.native.cRecord, this.converted.cRecord);
        }

        /// <summary>
        /// Check the conversion of columnidindexname.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidindexname")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidindexname()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidindexname }, this.converted.columnidindexname);
        }

        /// <summary>
        /// Check the conversion of columnidgrbitIndex.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidgrbitIndex")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidgrbitIndex()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidgrbitIndex }, this.converted.columnidgrbitIndex);
        }

        /// <summary>
        /// Check the conversion of columnidcKey.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcKey")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcKey()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcKey }, this.converted.columnidcKey);
        }

        /// <summary>
        /// Check the conversion of columnidcEntry.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcEntry")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcEntry()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcEntry }, this.converted.columnidcEntry);
        }

        /// <summary>
        /// Check the conversion of columnidcPage.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcPage")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcPage()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcPage }, this.converted.columnidcPage);
        }

        /// <summary>
        /// Check the conversion of columnidcColumn.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcColumn")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcColumn()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcColumn }, this.converted.columnidcColumn);
        }

        /// <summary>
        /// Check the conversion of columnidiColumn.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidiColumn")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidiColumn()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidiColumn }, this.converted.columnidiColumn);
        }

        /// <summary>
        /// Check the conversion of columnidcolumnid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcolumnid")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcolumnid()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcolumnid }, this.converted.columnidcolumnid);
        }

        /// <summary>
        /// Check the conversion of columnidcoltyp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcoltyp")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcoltyp()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcoltyp }, this.converted.columnidcoltyp);
        }

        /// <summary>
        /// Check the conversion of columnidLangid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidLangid")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidLangid()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidLangid }, this.converted.columnidLangid);
        }

        /// <summary>
        /// Check the conversion of columnidCp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidCp")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidCp()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidCp }, this.converted.columnidCp);
        }

        /// <summary>
        /// Check the conversion of columnidgrbitColumn.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidgrbitColumn")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidgrbitColumn()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidgrbitColumn }, this.converted.columnidgrbitColumn);
        }
 
        /// <summary>
        /// Check the conversion of columnidcolumnname.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidcolumnname")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidcolumnname()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidcolumnname }, this.converted.columnidcolumnname);
        }

        /// <summary>
        /// Check the conversion of columnidLCMapFlags.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting a NATIVE_INDEXLIST to a JET_INDEXLIST sets columnidLCMapFlags")]
        public void VerifyConvertIndexlistFromNativeSetsColumnidLcMapFlags()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = this.native.columnidLCMapFlags }, this.converted.columnidLCMapFlags);
        }
    }
}