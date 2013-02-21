//-----------------------------------------------------------------------
// <copyright file="UtilTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the Util class.
    /// </summary>
    [TestClass]
    public class UtilTests
    {
        /// <summary>
        /// Test DumpBytes with a null array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a null array")]
        public void TestDumpBytesNull()
        {
            Assert.AreEqual("<null>", Util.DumpBytes(null, 0, 0));
        }

        /// <summary>
        /// Test DumpBytes with a zero-length array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a zero-length array")]
        public void TestDumpBytesZeroLength()
        {
            Assert.AreEqual(String.Empty, Util.DumpBytes(new byte[0], 0, 0));
        }

        /// <summary>
        /// Test DumpBytes with a negative offset.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a negative offset")]
        public void TestDumpBytesNegativeOffset()
        {
            Assert.AreEqual("<invalid>", Util.DumpBytes(new byte[1], -1, 1));
        }

        /// <summary>
        /// Test DumpBytes with an offset past the start of the array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with an offset past the start of the array")]
        public void TestDumpBytesInvalidOffset()
        {
            Assert.AreEqual("<invalid>", Util.DumpBytes(new byte[1], 2, 1));
        }

        /// <summary>
        /// Test DumpBytes with a negative count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a negative count")]
        public void TestDumpBytesNegativeCount()
        {
            Assert.AreEqual("<invalid>", Util.DumpBytes(new byte[2], 2, -1));
        }

        /// <summary>
        /// Test DumpBytes with a count past the end of the array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a count past the end of the array")]
        public void TestDumpBytesInvalidCount()
        {
            Assert.AreEqual("<invalid>", Util.DumpBytes(new byte[1], 1, 2));
        }

        /// <summary>
        /// Test DumpBytes with a short array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a short array")]
        public void TestDumpBytesShortArray()
        {
            Assert.AreEqual("DD-CC-BB-AA", Util.DumpBytes(BitConverter.GetBytes(0xAABBCCDD), 0, 4));
        }

        /// <summary>
        /// Test DumpBytes with an offset.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with an offset")]
        public void TestDumpBytesOffset()
        {
            Assert.AreEqual("CC-BB", Util.DumpBytes(BitConverter.GetBytes(0xAABBCCDD), 1, 2));
        }

        /// <summary>
        /// Test DumpBytes with a truncated array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DumpBytes with a truncated array")]
        public void TestDumpBytesTruncatedArray()
        {
            var b = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9 };
            Assert.AreEqual("00-01-02-03-04-05-06-07... (10 bytes)", Util.DumpBytes(b, 0, b.Length));
        }
            
        /// <summary>
        /// Test ArrayEqual with equal arrays.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test ArrayEqual with equal arrays")]
        public void TestArrayEqualTrue()
        {
            byte[] a = new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6 };
            byte[] b = new byte[] { 0xF, 0x2, 0x3, 0x4, 0x5, 0xF, 0x0 };
            Assert.IsTrue(Util.ArrayEqual(a, b, 1, 4));
        }

        /// <summary>
        /// Test ArrayEqual with unequal arrays.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test ArrayEqual with unequal arrays")]
        public void TestArrayEqualFalse()
        {
            byte[] a = new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6 };
            byte[] b = new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6 };

            for (int offset = 0; offset < a.Length - 1; ++offset)
            {
                for (int count = 1; count < a.Length - offset; ++count)
                {
                    for (int i = 0; i < count; ++i)
                    {
                        b[offset + i] ^= 0xFF;
                        Assert.IsFalse(
                            Util.ArrayEqual(a, b, offset, count),
                            "{0} is equal to {1} (offset = {2}, count = {3})",
                            BitConverter.ToString(a),
                            BitConverter.ToString(b),
                            offset,
                            count);
                        b[offset + i] ^= 0xFF;
                    }
                }
            }
        }

        /// <summary>
        /// Check that ArrayObjectContentEquals compares null arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayObjectContentEquals compares null arrays correctly")]
        public void TestArrayObjectContentEqualsNullArrays()
        {
            Assert.IsTrue(Util.ArrayObjectContentEquals<JET_SPACEHINTS>(null, null, 0)); 
        }

        /// <summary>
        /// Check that ArrayObjectContentEquals compares null entries correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayObjectContentEquals compares null members correctly")]
        public void TestArrayObjectContentEqualsNullMembers()
        {
            var x = new JET_SPACEHINTS[] { null, null };
            var y = new JET_SPACEHINTS[] { null, null };

            Assert.IsTrue(Util.ArrayObjectContentEquals(x, y, x.Length));
        }

        /// <summary>
        /// Check that ArrayObjectContentEquals compares differing arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayObjectContentEquals compares differeing arrays correctly")]
        public void TestArrayObjectContentEqualsDiffer()
        {
            var a = new[]
            {
                new JET_SPACEHINTS
                {
                    ulInitialDensity = 33,
                    cbInitial = 4096,
                }
            };

            var b = new[]
            {
                new JET_SPACEHINTS
                {
                    ulInitialDensity = 34,
                    cbInitial = 4096,
                }
            };

            var c = new[]
            {
                new JET_SPACEHINTS
                {
                    ulInitialDensity = 33,
                    cbInitial = 4077,
                }
            };

            var d = new JET_SPACEHINTS[]
            {
                null,
            };

            JET_SPACEHINTS[][] values = new[] { a, b, c, d, null };

            for (int i = 0; i < values.Length - 1; i++)
            {
                for (int j = i + 1; j < values.Length; j++)
                {
                    Assert.IsFalse(Util.ArrayObjectContentEquals(values[i], values[j], 1));
                }
            }
        }

        /// <summary>
        /// Check that ArrayObjectContentEquals compares identical arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayObjectContentEquals compares identical arrays correctly")]
        public void TestArrayObjectContentEqualsIdentical()
        {
            var x = new[]
            {
                new JET_SPACEHINTS
                {
                    ulInitialDensity = 33,
                    cbInitial = 4096,
                }
            };

            var y = new[]
            {
                new JET_SPACEHINTS
                {
                    ulInitialDensity = 33,
                    cbInitial = 4096,
                }
            };

            var z = new[]
            {
                x[0],
            };

            JET_SPACEHINTS[][] values = new[] { x, y, z };

            for (int i = 0; i < values.Length; i++)
            {
                for (int j = 0; j < values.Length; j++)
                {
                    Assert.IsTrue(Util.ArrayObjectContentEquals(values[i], values[j], 1));
                }
            }
        }

        /// <summary>
        /// Check that ArrayStructEquals compares null arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayStructEquals compares null arrays correctly")]
        public void TestArrayStructEqualsNullArrays()
        {
            Assert.IsTrue(Util.ArrayStructEquals<byte>(null, null, 0));
        }

        /// <summary>
        /// Check that ArrayStructEquals compares the same array correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayStructEquals compares the same array correctly")]
        public void TestArrayStructEqualsSameArrays()
        {
            var x = new long[] { 0x1, 0x2, 0x3, 0x4 };
            Assert.IsTrue(Util.ArrayStructEquals(x, x, x.Length));
        }

        /// <summary>
        /// Check that ArrayStructEquals compares equal arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayStructEquals compares equal arrays correctly")]
        public void TestArrayStructEqualsEqualArrays()
        {
            Assert.IsTrue(Util.ArrayStructEquals(new byte[] { 0x1, 0x2 }, new byte[] { 0x1, 0x2, 0x3 }, 2));
        }

        /// <summary>
        /// Check that ArrayStructEquals comparesun equal arrays correctly.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that ArrayStructEquals compares unequal arrays correctly")]
        public void TestArrayStructEqualsUnequalArrays()
        {
            Assert.IsFalse(Util.ArrayStructEquals(new byte[] { 0x1, 0x3 }, new byte[] { 0x1, 0x2 }, 2));
        }

        /// <summary>
        /// Check that AddTrailingDirectorySeparator returns null when given
        /// a null string.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test AddTrailingDirectorySeparator with null")]
        public void TestAddTrailingDirectorySeparatorNullString()
        {
            Assert.IsNull(Util.AddTrailingDirectorySeparator(null));
        }

        /// <summary>
        /// Check that AddTrailingDirectorySeparator returns an empty string when given
        /// an empty string.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test AddTrailingDirectorySeparator with an empty string")]
        public void TestAddTrailingDirectorySeparatorEmptyString()
        {
            Assert.AreEqual(String.Empty, Util.AddTrailingDirectorySeparator(String.Empty));
        }

        /// <summary>
        /// Check that AddTrailingDirectorySeparator returns a terminated string.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test AddTrailingDirectorySeparator with a string that has the separator")]
        public void TestAddTrailingDirectorySeparatorTerminatedString()
        {
            string expected = @"foo\";
            Assert.AreEqual(expected, Util.AddTrailingDirectorySeparator(expected));
        }

        /// <summary>
        /// Check that AddTrailingDirectorySeparator returns a terminated string.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test AddTrailingDirectorySeparator with a string that does not have the separator")]
        public void TestAddTrailingDirectorySeparatorNonTerminatedString()
        {
            string expected = @"foo\bar\";
            Assert.AreEqual(expected, Util.AddTrailingDirectorySeparator(@"foo\bar"));
        }
    }
}