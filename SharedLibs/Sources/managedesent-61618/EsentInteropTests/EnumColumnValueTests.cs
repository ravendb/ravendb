//-----------------------------------------------------------------------
// <copyright file="EnumColumnValueTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JET_ENUMCOLUMNVALUE conversion.
    /// </summary>
    [TestClass]
    public class EnumColumnValueTests
    {
        /// <summary>
        /// Test conversion from native to managed.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_ENUMCOLUMNVALUE to JET_ENUMCOLUMNVALUE")]
        public void TestConversion()
        {
            var native = new NATIVE_ENUMCOLUMNVALUE
            {
                cbData = 1,
                err = (int)JET_wrn.ColumnTruncated,
                itagSequence = 2,
                pvData = new IntPtr(3),
            };

            var managed = new JET_ENUMCOLUMNVALUE();
            managed.SetFromNativeEnumColumnValue(native);

            Assert.AreEqual(1, managed.cbData);
            Assert.AreEqual(JET_wrn.ColumnTruncated, managed.err);
            Assert.AreEqual(2, managed.itagSequence);
            Assert.AreEqual(new IntPtr(3), managed.pvData);
        }
    }
}
