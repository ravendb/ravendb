//-----------------------------------------------------------------------
// <copyright file="ColumnlistTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_COLUMNLIST tests
    /// </summary>
    [TestClass]
    public class ColumnlistTests
    {
        /// <summary>
        /// Test conversion from NATIVE_COLUMNLIST to JET_COLUMNLIST.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNLIST to JET_COLUMNLIST.")]
        public void ConvertColumnlistFromNative()
        {
            var tableid = new JET_TABLEID { Value = (IntPtr)0x1000 };
            var col1 = new JET_COLUMNID { Value = 1 };
            var col2 = new JET_COLUMNID { Value = 2 };
            var col3 = new JET_COLUMNID { Value = 3 };
            var col4 = new JET_COLUMNID { Value = 4 };
            var col5 = new JET_COLUMNID { Value = 5 };
            var col6 = new JET_COLUMNID { Value = 6 };
            var col7 = new JET_COLUMNID { Value = 7 };

            var native = new NATIVE_COLUMNLIST()
            {
                tableid = tableid.Value,
                cRecord = 100,
                columnidcolumnname = col1.Value,
                columnidcolumnid = col2.Value,
                columnidcoltyp = col3.Value,
                columnidCp = col4.Value,
                columnidcbMax = col5.Value,
                columnidgrbit = col6.Value,
                columnidDefault = col7.Value,
            };

            var columnlist = new JET_COLUMNLIST();
            columnlist.SetFromNativeColumnlist(native);

            Assert.AreEqual(tableid, columnlist.tableid);
            Assert.AreEqual(100, columnlist.cRecord);
            Assert.AreEqual(col1, columnlist.columnidcolumnname);
            Assert.AreEqual(col2, columnlist.columnidcolumnid);
            Assert.AreEqual(col3, columnlist.columnidcoltyp);
            Assert.AreEqual(col4, columnlist.columnidCp);
            Assert.AreEqual(col5, columnlist.columnidcbMax);
            Assert.AreEqual(col6, columnlist.columnidgrbit);
            Assert.AreEqual(col7, columnlist.columnidDefault);
        }
    }
}