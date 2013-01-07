//-----------------------------------------------------------------------
// <copyright file="ObjectInfoConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_OBJECTINFO conversion tests.
    /// </summary>
    [TestClass]
    public class ObjectInfoConversionTests
    {
        /// <summary>
        /// The native OBJECTINFO that will be converted to managed.
        /// </summary>
        private NATIVE_OBJECTINFO native;

        /// <summary>
        /// The managed OBJECTINFO created from the native.
        /// </summary>
        private JET_OBJECTINFO managed;

        /// <summary>
        /// Create a native OBJECTINFO and convert it to managed.
        /// </summary>
        [TestInitialize]
        [Description("Setup the ObjectInfoConversionTests test fixture")]
        public void Setup()
        {
            this.native = new NATIVE_OBJECTINFO()
            {
                cbStruct = (uint)Marshal.SizeOf(typeof(NATIVE_OBJECTINFO)),
                cPage = 2,
                cRecord = 3,
                flags = 0x20000000,     // Template
                grbit = 7,              // Updatable | Bookmark | Rollback
                objtyp = 1,             // Table
            };

            this.managed = new JET_OBJECTINFO();
            this.managed.SetFromNativeObjectinfo(ref this.native);
        }

        /// <summary>
        /// Test conversion from the native stuct sets cPage.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTINFO to JET_OBJECTINFO sets cPage")]
        public void ConvertObjectInfoFromNativeSetsCPage()
        {
            Assert.AreEqual(2, this.managed.cPage);
        }

        /// <summary>
        /// Test conversion from the native stuct sets cRecord.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTINFO to JET_OBJECTINFO sets cRecord")]
        public void ConvertObjectInfoFromNativeSetsCRecord()
        {
            Assert.AreEqual(3, this.managed.cRecord);
        }

        /// <summary>
        /// Test conversion from the native stuct sets flags.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTINFO to JET_OBJECTINFO sets flags")]
        public void ConvertObjectInfoFromNativeSetFlags()
        {
            Assert.AreEqual(ObjectInfoFlags.TableTemplate, this.managed.flags);
        }

        /// <summary>
        /// Test conversion from the native stuct sets grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTINFO to JET_OBJECTINFO sets grbit")]
        public void ConvertObjectInfoFromNativeSetGrbit()
        {
            Assert.AreEqual(
                ObjectInfoGrbit.Bookmark | ObjectInfoGrbit.Rollback | ObjectInfoGrbit.Updatable,
                this.managed.grbit);
        }

        /// <summary>
        /// Test conversion from the native stuct sets objtyp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTINFO to JET_OBJECTINFO sets objtyp")]
        public void ConvertObjectInfoFromNativeSetObjtyp()
        {
            Assert.AreEqual(JET_objtyp.Table, this.managed.objtyp);
        }
    }
}