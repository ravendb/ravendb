//-----------------------------------------------------------------------
// <copyright file="ObjectlistConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_OBJECTLIST conversion tests.
    /// </summary>
    [TestClass]
    public class ObjectlistConversionTests
    {
        /// <summary>
        /// The native objectlist that will be converted to managed.
        /// </summary>
        private NATIVE_OBJECTLIST native;

        /// <summary>
        /// The managed objectlist created from the native.
        /// </summary>
        private JET_OBJECTLIST managed;

        /// <summary>
        /// Create a native objectlist and convert it to managed.
        /// </summary>
        [TestInitialize]
        [Description("Setup the ObjectlistConversionTests test fixture")]
        public void Setup()
        {
            this.native = new NATIVE_OBJECTLIST()
            {
                tableid = new IntPtr(0x100),
                cRecord = 100,
                columnidobjectname = 1,
                columnidobjtyp = 2,
                columnidgrbit = 3,
                columnidflags = 4,
                columnidcRecord = 5,
                columnidcPage = 6,
            };

            this.managed = new JET_OBJECTLIST();
            this.managed.SetFromNativeObjectlist(this.native);            
        }

        /// <summary>
        /// Test conversion from the native stuct sets tableid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets tableid")]
        public void ConvertObjectlistFromNativeSetsTableid()
        {
            var tableid = new JET_TABLEID { Value = this.native.tableid };
            Assert.AreEqual(tableid, this.managed.tableid);
        }

        /// <summary>
        /// Test conversion from the native stuct sets cRecord.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets cRecord")]
        public void ConvertObjectlistFromNativeSetsCrecord()
        {
            Assert.AreEqual((int)this.native.cRecord, this.managed.cRecord);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidobjectname.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidobjectname")]
        public void ConvertObjectlistFromNativeSetsColumnidobjectname()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidobjectname };
            Assert.AreEqual(column, this.managed.columnidobjectname);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidobjtyp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidobjtyp")]
        public void ConvertObjectlistFromNativeSetsColumnidobjtyp()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidobjtyp };
            Assert.AreEqual(column, this.managed.columnidobjtyp);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidgrbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidgrbit")]
        public void ConvertObjectlistFromNativeSetsColumnidgrbit()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidgrbit };
            Assert.AreEqual(column, this.managed.columnidgrbit);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidflags.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidflags")]
        public void ConvertObjectlistFromNativeSetsColumnidflags()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidflags };
            Assert.AreEqual(column, this.managed.columnidflags);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidcRecord.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidcRecord")]
        public void ConvertObjectlistFromNativeSetsColumnidCrecord()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidcRecord };
            Assert.AreEqual(column, this.managed.columnidcRecord);
        }

        /// <summary>
        /// Test conversion from the native stuct sets columnidcPage.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversation from NATIVE_OBJECTLIST to JET_OBJECTLIST sets columnidcPage")]
        public void ConvertObjectlistFromNativeSetsColumnidCpage()
        {
            var column = new JET_COLUMNID { Value = this.native.columnidcPage };
            Assert.AreEqual(column, this.managed.columnidcPage);
        }
    }
}