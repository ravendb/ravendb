//-----------------------------------------------------------------------
// <copyright file="TableCreate2Tests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_TABLECREATE2
    /// </summary>
    [TestClass]
    public class TableCreate2Tests
    {
        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_TABLECREATE managed;

        /// <summary>
        /// The native conditional column structure created from the JET_TABLECREATE
        /// object.
        /// </summary>
        private NATIVE_TABLECREATE2 native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the Indexcreate2Tests fixture")]
        public void Setup()
        {
            JET_TABLEID tableidTemp = new JET_TABLEID()
            {
                Value = (IntPtr)456,
            };
            this.managed = new JET_TABLECREATE()
            {
                szTableName = "table7",
                szTemplateTableName = "parentTable",
                ulPages = 7,
                ulDensity = 62,
                rgcolumncreate = null,
                cColumns = 0,
                rgindexcreate = null,
                cIndexes = 0,
                szCallback = "module!FunkyFunction",
                cbtyp = JET_cbtyp.AfterReplace,
                grbit = CreateTableColumnIndexGrbit.FixedDDL,
                pSeqSpacehints = null,
                pLVSpacehints = null,
                cbSeparateLV = 0x999,
                tableid = tableidTemp,
                cCreated = 2,
            };

            this.native = this.managed.GetNativeTableCreate2();
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets the structure size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets the structure size.")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets the table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets the table name.")]
        public void VerifyConversionToNativeSetsName()
        {
            Assert.AreEqual("table7", this.native.szTableName);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets szTemplateTableName.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets szTemplateTableName.")]
        public void VerifyConversionToNativeSetsSzTemplateTableName()
        {
            Assert.AreEqual("parentTable", this.native.szTemplateTableName);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets ulPages.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets ulPages.")]
        public void VerifyConversionToNativeSetsUlPages()
        {
            Assert.AreEqual<uint>(7, this.native.ulPages);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets ulDensity.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets ulDensity.")]
        public void VerifyConversionToNativeSetsUlDensity()
        {
            Assert.AreEqual<uint>(62, this.native.ulDensity);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets rgcolumncreate.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets rgcolumncreate.")]
        public unsafe void VerifyConversionToNativeSetsRgcolumncreate()
        {
            Assert.IsTrue(this.native.rgcolumncreate == null);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets cColumns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets cColumns.")]
        public void VerifyConversionToNativeSetsCColumns()
        {
            Assert.AreEqual<uint>(0, this.native.cColumns);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets rgindexcreate.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets rgindexcreate.")]
        public void VerifyConversionToNativeSetsRgindexcreate()
        {
            Assert.AreEqual(this.native.rgindexcreate, IntPtr.Zero);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets cIndexes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets cIndexes.")]
        public void VerifyConversionToNativeSetsCIndexes()
        {
            Assert.AreEqual<uint>(0, this.native.cIndexes);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets szCallback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets szCallback.")]
        public void VerifyConversionToNativeSetsSzCallback()
        {
            Assert.AreEqual("module!FunkyFunction", this.native.szCallback);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets cbtyp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets cbtyp.")]
        public void VerifyConversionToNativeSetsCbtyp()
        {
            Assert.AreEqual(0x10 /*JET_cbtyp.AfterReplace*/, (int)this.native.cbtyp);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets tableid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets tableid.")]
        public void VerifyConversionToNativeSetsTableid()
        {
            Assert.AreEqual<IntPtr>((IntPtr)456, this.native.tableid);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_TABLECREATE2 sets cCreated.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_TABLECREATE to a NATIVE_TABLECREATE2 sets cCreated.")]
        public void VerifyConversionToNativeSetsCCreated()
        {
            Assert.AreEqual<uint>(2, this.native.cCreated);
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches negative cColumns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches negative cColumns.")]
        public void VerifyValidityCatchesNegativeCColumns()
        {
            var x = new JET_TABLECREATE()
            {
                rgcolumncreate = null,
                cColumns = -1,
            };

            var y = new JET_TABLECREATE();
            Assert.IsFalse(x.ContentEquals(y));
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches negative cIndexes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches negative cIndexes.")]
        public void VerifyValidityCatchesNegativeCIndexes()
        {
            var x = new JET_TABLECREATE()
            {
                rgcolumncreate = null,
                cIndexes = -1,
            };

            var y = new JET_TABLECREATE();
            Assert.IsFalse(x.ContentEquals(y));
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches cIndexes that's too big.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches cIndexes that's too big.")]
        public void VerifyValidityCatchesCIndexesTooBig()
        {
            var x = new JET_TABLECREATE()
            {
                rgindexcreate = new JET_INDEXCREATE[]
                {
                    new JET_INDEXCREATE(),
                    new JET_INDEXCREATE(),
                },
                cIndexes = 10,
            };

            var y = new JET_TABLECREATE();
            Assert.IsFalse(x.ContentEquals(y));
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches cColumns that's too big.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches cColumns that's too big.")]
        public void VerifyValidityCatchesCColumnsTooBig()
        {
            var x = new JET_TABLECREATE()
            {
                rgcolumncreate = new JET_COLUMNCREATE[]
                {
                    new JET_COLUMNCREATE(),
                    new JET_COLUMNCREATE(),
                },
                cColumns = 10,
            };

            var y = new JET_TABLECREATE();
            Assert.IsFalse(x.ContentEquals(y));
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches non-zero cColumns when the array is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches non-zero cColumns when the array is null.")]
        public void VerifyValidityCatchesNonZeroCColumnsWithNullArray()
        {
            var x = new JET_TABLECREATE()
            {
                rgcolumncreate = null,
                cColumns = 10,
            };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches non-zero cIndexes when the array is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches non-zero cIndexes when the array is null.")]
        public void VerifyValidityCatchesNonZeroCIndexesWithNullArray()
        {
            var x = new JET_TABLECREATE()
            {
                rgindexcreate = null,
                cIndexes = 10,
            };
            x.CheckMembersAreValid();
        }
    }
}