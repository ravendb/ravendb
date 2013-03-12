//-----------------------------------------------------------------------
// <copyright file="EnumColumnidTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JET_ENUMCOLUMNID conversion and checking.
    /// </summary>
    [TestClass]
    public class EnumColumnidTests
    {
        /// <summary>
        /// When ctagSequence is negative we should throw an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("When JET_ENUMCOLUMNID.ctagSequence is negative we should throw an exception")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestConvertEnumColumnidCheckThrowsExceptionWhenCTagsSequenceIsNegative()
        {
            var managed = new JET_ENUMCOLUMNID
            {
                columnid = new JET_COLUMNID { Value = 1 },
                ctagSequence = -1,
                rgtagSequence = null,
            };

            managed.CheckDataSize();
        }

        /// <summary>
        /// When ctagSequence is greater than the length of rgtagSequence we should 
        /// throw an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("When JET_ENUMCOLUMNID.ctagSequence is greater than the length of rgtagSequence we should throw an exception")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestConvertEnumColumnidCheckThrowsExceptionWhenCTagsSequenceIsTooLong()
        {
            var managed = new JET_ENUMCOLUMNID
            {
                columnid = new JET_COLUMNID { Value = 1 },
                ctagSequence = 3,
                rgtagSequence = new int[2],
            };

            managed.CheckDataSize();
        }

        /// <summary>
        /// Non-zero ctagSequence and null rgtagSequence should throw an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Non-zero JET_ENUMCOLUMNID.ctagSequence and null rgtagSequence should throw an exception")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestConvertEnumColumnidCheckThrowsExceptionWhenRgtagSequenceIsUnexpectedNull()
        {
            var managed = new JET_ENUMCOLUMNID
            {
                columnid = new JET_COLUMNID { Value = 1 },
                ctagSequence = 1,
                rgtagSequence = null,
            };

            managed.CheckDataSize();
        }

        /// <summary>
        /// Test conversion from managed to native.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting JET_ENUMCOLUMNID from managed to native")]
        public void TestConvertEnumColumnidToNativeWithNoTags()
        {
            var managed = new JET_ENUMCOLUMNID
            {
                columnid = new JET_COLUMNID { Value = 1 },
                ctagSequence = 0,
                rgtagSequence = null,
            };

            var native = managed.GetNativeEnumColumnid();
            Assert.AreEqual<uint>(1, native.columnid);
            Assert.AreEqual<uint>(0, native.ctagSequence);
            unsafe
            {
                Assert.IsTrue(null == native.rgtagSequence);
            }
        }
    }
}
