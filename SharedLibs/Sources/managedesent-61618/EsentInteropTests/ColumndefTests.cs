//-----------------------------------------------------------------------
// <copyright file="ColumndefTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_COLUMNDEF tests
    /// </summary>
    [TestClass]
    public class ColumndefTests
    {
        /// <summary>
        /// Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF.")]
        public void ConvertColumndefToNative()
        {
            var columndef = new JET_COLUMNDEF
            {
                cbMax = 0x1,
                coltyp = JET_coltyp.Binary,
                cp = JET_CP.Unicode,
                grbit = ColumndefGrbit.ColumnAutoincrement
            };

            NATIVE_COLUMNDEF native = columndef.GetNativeColumndef();
            Assert.AreEqual<uint>(0, native.columnid);
            Assert.AreEqual<uint>(9, native.coltyp);
#pragma warning disable 618,612 // Disable warning that wCountry/langid/wCollate are obsolete
            Assert.AreEqual<ushort>(0, native.wCountry);
            Assert.AreEqual<ushort>(0, native.langid);
            Assert.AreEqual<ushort>(1200, native.cp);
            Assert.AreEqual<ushort>(0, native.wCollate);
#pragma warning restore 618,612
            Assert.AreEqual<uint>(1, native.cbMax);
            Assert.AreEqual<uint>(0x10, native.grbit);
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNDEF to JET_COLUMNDEF.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNDEF to JET_COLUMNDEF.")]
        public void ConvertColumndefFromNative()
        {
            var native = new NATIVE_COLUMNDEF()
            {
                cbMax = 1,
                coltyp = (uint)JET_coltyp.LongText,
                columnid = 0x100,
                cp = 1200,
                grbit = (uint)ColumndefGrbit.ColumnMultiValued,
            };

            var columndef = new JET_COLUMNDEF();
            columndef.SetFromNativeColumndef(native);
            Assert.AreEqual(1, columndef.cbMax);
            Assert.AreEqual(JET_coltyp.LongText, columndef.coltyp);
            Assert.AreEqual<uint>(0x100, columndef.columnid.Value);
            Assert.AreEqual(JET_CP.Unicode, columndef.cp);
            Assert.AreEqual(ColumndefGrbit.ColumnMultiValued, columndef.grbit);
        }
    }
}
