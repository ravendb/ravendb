//-----------------------------------------------------------------------
// <copyright file="EnumColumnTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JET_ENUMCOLUMN conversion.
    /// </summary>
    [TestClass]
    public class EnumColumnTests
    {
        /// <summary>
        /// Test conversion of a single value
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of a single value in a NATIVE_ENUMCOLUMN")]
        public void TestSingleValueConversion()
        {
            var native = new NATIVE_ENUMCOLUMN
            {
                columnid = 1,
                err = (int)JET_wrn.ColumnSingleValue,
                cbData = 3,
                pvData = new IntPtr(4),
            };

            var managed = new JET_ENUMCOLUMN();
            managed.SetFromNativeEnumColumn(native);

            Assert.AreEqual<uint>(1, managed.columnid.Value);
            Assert.AreEqual(JET_wrn.ColumnSingleValue, managed.err);
            Assert.AreEqual(3, managed.cbData);
            Assert.AreEqual(new IntPtr(4), managed.pvData);
        }

        /// <summary>
        /// Test conversion of a multi value
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of a multi value in JET_ENUMCOLUMN")]
        public void TestMultiValueConversion()
        {
            var managed = new JET_ENUMCOLUMN();
            unsafe
            {
                var columnvalues = stackalloc NATIVE_ENUMCOLUMNVALUE[2];
                var native = new NATIVE_ENUMCOLUMN
                {
                    columnid = 1,
                    err = (int)JET_wrn.Success,
                    cEnumColumnValue = 3,
                    rgEnumColumnValue = columnvalues,
                };

                managed.SetFromNativeEnumColumn(native);                
            }

            Assert.AreEqual<uint>(1, managed.columnid.Value);
            Assert.AreEqual(JET_wrn.Success, managed.err);
            Assert.AreEqual(3, managed.cEnumColumnValue);
            Assert.AreEqual(null, managed.rgEnumColumnValue);
        }
    }
}
