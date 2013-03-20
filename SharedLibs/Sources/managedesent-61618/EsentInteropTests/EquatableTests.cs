//-----------------------------------------------------------------------
// <copyright file="EquatableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for classes that implement IEquatable
    /// </summary>
    [TestClass]
    public class EquatableTests
    {
        /// <summary>
        /// Check that JET_INSTANCE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INSTANCE structures can be compared for equality")]
        public void VerifyJetInstanceEquality()
        {
            var x = JET_INSTANCE.Nil;
            var y = JET_INSTANCE.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_INSTANCE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INSTANCE structures can be compared for inequality")]
        public void VerifyJetInstanceInequality()
        {
            var x = JET_INSTANCE.Nil;
            var y = new JET_INSTANCE { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_SESID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SESID structures can be compared for equality")]
        public void VerifyJetSesidEquality()
        {
            var x = JET_SESID.Nil;
            var y = JET_SESID.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_SESID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SESID structures can be compared for inequality")]
        public void VerifyJetSesidInequality()
        {
            var x = JET_SESID.Nil;
            var y = new JET_SESID { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_TABLEID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_TABLEID structures can be compared for equality")]
        public void VerifyJetTableidEquality()
        {
            var x = JET_TABLEID.Nil;
            var y = JET_TABLEID.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_TABLEID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_TABLEID structures can be compared for inequality")]
        public void VerifyJetTableidInequality()
        {
            var x = JET_TABLEID.Nil;
            var y = new JET_TABLEID { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_DBID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_DBID structures can be compared for equality")]
        public void VerifyJetDbidEquality()
        {
            var x = JET_DBID.Nil;
            var y = JET_DBID.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_DBID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_DBID structures can be compared for inequality")]
        public void VerifyJetDbidInequality()
        {
            var x = JET_DBID.Nil;
            var y = new JET_DBID { Value = 0x2 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_COLUMNID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNID structures can be compared for equality")]
        public void VerifyJetColumnidEquality()
        {
            var x = JET_COLUMNID.Nil;
            var y = JET_COLUMNID.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_COLUMNID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNID structures can be compared for inequality")]
        public void VerifyJetColumnidInequality()
        {
            var x = JET_COLUMNID.Nil;
            var y = new JET_COLUMNID { Value = 0xF };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_OSSNAPID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_OSSNAPID structures can be compared for equality")]
        public void VerifyJetOsSnapidEquality()
        {
            var x = JET_OSSNAPID.Nil;
            var y = JET_OSSNAPID.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_OSSNAPID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_OSSNAPID structures can be compared for inequality")]
        public void VerifyJetOsSnapidInequality()
        {
            var x = JET_OSSNAPID.Nil;
            var y = new JET_OSSNAPID { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_HANDLE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_HANDLE structures can be compared for equality")]
        public void VerifyJetHandleEquality()
        {
            var x = JET_HANDLE.Nil;
            var y = JET_HANDLE.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_HANDLE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_HANDLE structures can be compared for inequality")]
        public void VerifyJetHandleInequality()
        {
            var x = JET_HANDLE.Nil;
            var y = new JET_HANDLE { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_LS structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LS structures can be compared for equality")]
        public void VerifyJetLsEquality()
        {
            var x = JET_LS.Nil;
            var y = JET_LS.Nil;
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_LS structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LS structures can be compared for inequality")]
        public void VerifyJetLsInequality()
        {
            var x = JET_LS.Nil;
            var y = new JET_LS { Value = (IntPtr)0x7 };
            TestNotEquals(x, y);
            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that JET_INDEXID structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXID structures can be compared for equality")]
        public void VerifyJetIndexIdEquality()
        {
            var x = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x2, IndexId3 = 0x3 };
            var y = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x2, IndexId3 = 0x3 };
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_INDEXID structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXID structures can be compared for inequality")]
        public void VerifyJetIndexIdInequality()
        {
            var x = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x2, IndexId3 = 0x3 };
            var y = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x22, IndexId3 = 0x3 };
            var z = new JET_INDEXID { IndexId1 = (IntPtr)0x1, IndexId2 = 0x2, IndexId3 = 0x33 };

            TestNotEquals(x, y);
            TestNotEquals(x, z);
            TestNotEquals(y, z);

            Assert.IsTrue(x != y);
            Assert.IsFalse(x == y);
        }

        /// <summary>
        /// Check that null JET_LOGTIME structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that null JET_LOGTIME structures can be compared for equality")]
        public void VerifyNullJetLogtimeEquality()
        {
            var x = new JET_LOGTIME();
            var y = new JET_LOGTIME();
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_LOGTIME structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LOGTIME structures can be compared for equality")]
        public void VerifyJetLogtimeEquality()
        {
            DateTime t = DateTime.Now;
            var x = new JET_LOGTIME(t);
            var y = new JET_LOGTIME(t);
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_LOGTIME structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LOGTIME structures can be compared for inequality")]
        public void VerifyJetLogtimeInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var times = new[]
            {
                new JET_LOGTIME(new DateTime(2010, 5, 31, 4, 44, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2011, 5, 31, 4, 44, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 7, 31, 4, 44, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 5, 30, 4, 44, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 5, 31, 5, 44, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 5, 31, 4, 45, 17, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 5, 31, 4, 44, 18, DateTimeKind.Utc)),
                new JET_LOGTIME(new DateTime(2010, 5, 31, 4, 44, 17, DateTimeKind.Local)),
                new JET_LOGTIME(),
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < times.Length - 1; ++i)
            {
                for (int j = i + 1; j < times.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_LOGTIME");
                    TestNotEquals(times[i], times[j]);
                    Assert.IsTrue(times[i] != times[j]);
                    Assert.IsFalse(times[i] == times[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_BKLOGTIME structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_BKLOGTIME structures can be compared for equality")]
        public void VerifyJetBklogtimeEquality()
        {
            DateTime t = DateTime.Now;
            var x = new JET_BKLOGTIME(t, false);
            var y = new JET_BKLOGTIME(t, false);
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_BKLOGTIME structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_BKLOGTIME structures can be compared for inequality")]
        public void VerifyJetBklogtimeInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var times = new[]
            {
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 4, 44, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 4, 44, 17, DateTimeKind.Utc), false),
                new JET_BKLOGTIME(new DateTime(2011, 5, 31, 4, 44, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 7, 31, 4, 44, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 30, 4, 44, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 5, 44, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 4, 45, 17, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 4, 44, 18, DateTimeKind.Utc), true),
                new JET_BKLOGTIME(new DateTime(2010, 5, 31, 4, 44, 17, DateTimeKind.Local), true),
                new JET_BKLOGTIME(),
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < times.Length - 1; ++i)
            {
                for (int j = i + 1; j < times.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_BKLOGTIME");
                    TestNotEquals(times[i], times[j]);
                    Assert.IsTrue(times[i] != times[j]);
                    Assert.IsFalse(times[i] == times[j]);
                }
            }
        }

        /// <summary>
        /// Check that null JET_SIGNATURE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that null JET_SIGNATURE structures can be compared for equality")]
        public void VerifyNullJetSignatureEquality()
        {
            var x = new JET_SIGNATURE();
            var y = new JET_SIGNATURE();
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_SIGNATURE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SIGNATURE structures can be compared for equality")]
        public void VerifyJetSignatureEquality()
        {
            DateTime t = DateTime.Now;
            var x = new JET_SIGNATURE(1, t, "COMPUTER");
            var y = new JET_SIGNATURE(1, t, "COMPUTER");
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_SIGNATURE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SIGNATURE structures can be compared for inequality")]
        public void VerifyJetSignatureInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            DateTime t = DateTime.UtcNow;
            var times = new[]
            {
                new JET_SIGNATURE(1, t, "COMPUTER"),
                new JET_SIGNATURE(2, t, "COMPUTER"),
                new JET_SIGNATURE(1, DateTime.Now, "COMPUTER"),
                new JET_SIGNATURE(1, null, "COMPUTER"),
                new JET_SIGNATURE(1, t, "COMPUTER2"),
                new JET_SIGNATURE(1, t, null),
                new JET_SIGNATURE(),
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < times.Length - 1; ++i)
            {
                for (int j = i + 1; j < times.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_SIGNATURE");
                    TestNotEquals(times[i], times[j]);
                    Assert.IsTrue(times[i] != times[j]);
                    Assert.IsFalse(times[i] == times[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_LGPOS structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LGPOS structures can be compared for equality")]
        public void VerifyJetLgposEquality()
        {
            var x = new JET_LGPOS { lGeneration = 1, isec = 2, ib = 3 };
            var y = new JET_LGPOS { lGeneration = 1, isec = 2, ib = 3 };
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_LGPOS structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LGPOS structures can be compared for inequality")]
        public void VerifyJetLgposInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var positions = new[]
            {
                new JET_LGPOS { lGeneration = 1, isec = 2, ib = 3 },
                new JET_LGPOS { lGeneration = 1, isec = 2, ib = 999 },
                new JET_LGPOS { lGeneration = 1, isec = 999, ib = 3 },
                new JET_LGPOS { lGeneration = 999, isec = 2, ib = 3 },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < positions.Length - 1; ++i)
            {
                for (int j = i + 1; j < positions.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_LGPOS");
                    TestNotEquals(positions[i], positions[j]);
                    Assert.IsTrue(positions[i] != positions[j]);
                    Assert.IsFalse(positions[i] == positions[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_BKINFO structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_BKINFO structures can be compared for equality")]
        public void VerifyJetBkinfoEquality()
        {
            var bklogtime = new JET_BKLOGTIME(DateTime.Now, false);
            var lgpos = new JET_LGPOS { lGeneration = 1, isec = 2, ib = 3 };

            var x = new JET_BKINFO { bklogtimeMark = bklogtime, genHigh = 11, genLow = 3, lgposMark = lgpos };
            var y = new JET_BKINFO { bklogtimeMark = bklogtime, genHigh = 11, genLow = 3, lgposMark = lgpos };
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_BKINFO structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_BKINFO structures can be compared for inequality")]
        public void VerifyJetBkinfoInequality()
        {
            var bklogtime1 = new JET_BKLOGTIME(DateTime.Now, false);
            var bklogtime2 = new JET_BKLOGTIME(DateTime.Now, true);
            var lgpos1 = new JET_LGPOS { lGeneration = 7, isec = 8, ib = 5 };
            var lgpos2 = new JET_LGPOS { lGeneration = 7, isec = 8, ib = 9 };

            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var positions = new[]
            {
                new JET_BKINFO { bklogtimeMark = bklogtime1, genHigh = 11, genLow = 3, lgposMark = lgpos1 },
                new JET_BKINFO { bklogtimeMark = bklogtime1, genHigh = 11, genLow = 3, lgposMark = lgpos2 },
                new JET_BKINFO { bklogtimeMark = bklogtime1, genHigh = 11, genLow = 4, lgposMark = lgpos1 },
                new JET_BKINFO { bklogtimeMark = bklogtime1, genHigh = 12, genLow = 3, lgposMark = lgpos1 },
                new JET_BKINFO { bklogtimeMark = bklogtime2, genHigh = 11, genLow = 3, lgposMark = lgpos1 },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < positions.Length - 1; ++i)
            {
                for (int j = i + 1; j < positions.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_LGPOS");
                    TestNotEquals(positions[i], positions[j]);
                    Assert.IsTrue(positions[i] != positions[j]);
                    Assert.IsFalse(positions[i] == positions[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_RECSIZE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RECSIZE structures can be compared for equality")]
        public void VerifyJetRecsizeEquality()
        {
            var x = new JET_RECSIZE
            {
                cbData = 1,
                cbDataCompressed = 2,
                cbLongValueData = 3,
                cbLongValueDataCompressed = 4,
                cbLongValueOverhead = 5,
                cbOverhead = 6,
                cCompressedColumns = 7,
                cLongValues = 8,
                cMultiValues = 9,
                cNonTaggedColumns = 10,
                cTaggedColumns = 11
            };
            var y = new JET_RECSIZE
            {
                cbData = 1,
                cbDataCompressed = 2,
                cbLongValueData = 3,
                cbLongValueDataCompressed = 4,
                cbLongValueOverhead = 5,
                cbOverhead = 6,
                cCompressedColumns = 7,
                cLongValues = 8,
                cMultiValues = 9,
                cNonTaggedColumns = 10,
                cTaggedColumns = 11
            };
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_RECSIZE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RECSIZE structures can be compared for inequality")]
        public void VerifyJetRecsizeInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var sizes = new[]
            {
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 11,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 12,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 13,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 14,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 15,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 16,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 17,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 18,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 19,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 20,
                    cTaggedColumns = 11
                },
                new JET_RECSIZE
                {
                    cbData = 1,
                    cbDataCompressed = 2,
                    cbLongValueData = 3,
                    cbLongValueDataCompressed = 4,
                    cbLongValueOverhead = 5,
                    cbOverhead = 6,
                    cCompressedColumns = 7,
                    cLongValues = 8,
                    cMultiValues = 9,
                    cNonTaggedColumns = 10,
                    cTaggedColumns = 21
                },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < sizes.Length - 1; ++i)
            {
                for (int j = i + 1; j < sizes.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_RECSIZE");
                    TestNotEquals(sizes[i], sizes[j]);
                    Assert.IsTrue(sizes[i] != sizes[j]);
                    Assert.IsFalse(sizes[i] == sizes[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_THREADSTATS structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_THREADSTATS structures can be compared for equality")]
        public void VerifyJetThreadstatsEquality()
        {
            var x = new JET_THREADSTATS
            {
                cbLogRecord = 1,
                cLogRecord = 2,
                cPageDirtied = 3,
                cPagePreread = 4,
                cPageRead = 5,
                cPageRedirtied = 6,
                cPageReferenced = 7,
            };
            var y = new JET_THREADSTATS
            {
                cbLogRecord = 1,
                cLogRecord = 2,
                cPageDirtied = 3,
                cPagePreread = 4,
                cPageRead = 5,
                cPageRedirtied = 6,
                cPageReferenced = 7,
            };
            TestEquals(x, y);
            Assert.IsTrue(x == y);
            Assert.IsFalse(x != y);
        }

        /// <summary>
        /// Check that JET_THREADSTATS structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_THREADSTATS structures can be compared for inequality")]
        public void VerifyJetThreadstatsInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var threadstats = new[]
            {
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 11,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 12,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 13,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 14,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 15,
                    cPageRedirtied = 6,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 16,
                    cPageReferenced = 7,
                },
                new JET_THREADSTATS
                {
                    cbLogRecord = 1,
                    cLogRecord = 2,
                    cPageDirtied = 3,
                    cPagePreread = 4,
                    cPageRead = 5,
                    cPageRedirtied = 6,
                    cPageReferenced = 17,
                },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for operator== and operator!=.
            for (int i = 0; i < threadstats.Length - 1; ++i)
            {
                for (int j = i + 1; j < threadstats.Length; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same JET_THREADSTATS");
                    TestNotEquals(threadstats[i], threadstats[j]);
                    Assert.IsTrue(threadstats[i] != threadstats[j]);
                    Assert.IsFalse(threadstats[i] == threadstats[j]);
                }
            }
        }

        /// <summary>
        /// Check that JET_SNPROG objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SNPROG objects can be compared for equality")]
        public void VerifyJetSnprogEquality()
        {
            var x = new JET_SNPROG { cunitDone = 1, cunitTotal = 2 };
            var y = new JET_SNPROG { cunitDone = 1, cunitTotal = 2 };
            TestEquals(x, y);

            // This is a reference class. Operator == and != still do reference comparisons.
        }

        /// <summary>
        /// Check that JET_SNPROG structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SNPROG objects can be compared for inequality")]
        public void VerifyJetSnprogInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var snprogs = new[]
            {
                new JET_SNPROG { cunitDone = 1, cunitTotal = 2 },
                new JET_SNPROG { cunitDone = 1, cunitTotal = 3 },
                new JET_SNPROG { cunitDone = 2, cunitTotal = 2 },
            };

            VerifyAll(snprogs);
        }

        /// <summary>
        /// Check that JET_INSTANCE_INFO objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INSTANCE_INFO objects can be compared for equality")]
        public void VerifyJetInstanceInfoEquality()
        {
            var x = new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "foo.edb" });
            var y = new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "foo.edb" });
            TestEquals(x, y);

            // This is a reference class. Operator == and != still do reference comparisons.
        }

        /// <summary>
        /// Check that JET_INSTANCE_INFO structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INSTANCE_INFO objects can be compared for inequality")]
        public void VerifyJetInstanceInfoInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var infos = new[]
            {
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "foo.edb" }),
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "foo.edb", "bar.edb" }),
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "bar.edb" }),
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", null),
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance2", new[] { "foo.edb" }),
                new JET_INSTANCE_INFO(JET_INSTANCE.Nil, null, new[] { "foo.edb" }),
                new JET_INSTANCE_INFO(new JET_INSTANCE { Value = new IntPtr(1) }, "instance", new[] { "foo.edb" }),
            };

            VerifyAll(infos);
        }

        /// <summary>
        /// Check that JET_DBINFOMISC objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JetDbinfoMisc objects can be compared for equality")]
        public void VerifyJetDbinfoMiscEquality()
        {
            var x = CreateJetDbinfoMisc();
            var y = CreateJetDbinfoMisc();
            TestEquals(x, y);
        }

        /// <summary>
        /// Check that JET_DBINFOMISC objects can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_DBINFOMISC objects can be compared for inequality")]
        public void VerifyJetDbinfoMiscInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var values = new JET_DBINFOMISC[41];
            for (int i = 0; i < values.Length; ++i)
            {
                values[i] = CreateJetDbinfoMisc();
            }

            int j = 1;
            values[j++].ulVersion++;
            values[j++].ulUpdate++;
            values[j++].signDb = new JET_SIGNATURE(0, DateTime.Now, "XYZZY");
            values[j++].dbstate = JET_dbstate.JustCreated;
            values[j++].lgposConsistent = new JET_LGPOS();
            values[j++].logtimeConsistent = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].logtimeAttach = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].lgposAttach = new JET_LGPOS();
            values[j++].logtimeDetach = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].lgposDetach = new JET_LGPOS();
            values[j++].signLog = new JET_SIGNATURE(0, DateTime.Now, "XYZZY");
            values[j++].bkinfoFullPrev = new JET_BKINFO();
            values[j++].bkinfoIncPrev = new JET_BKINFO();
            values[j++].bkinfoFullCur = new JET_BKINFO();
            values[j++].fShadowingDisabled = false;
            values[j++].fUpgradeDb = false;
            values[j++].dwMajorVersion++;
            values[j++].dwMinorVersion++;
            values[j++].dwBuildNumber++;
            values[j++].lSPNumber++;
            values[j++].cbPageSize++;
            values[j++].genMinRequired++;
            values[j++].genMaxRequired++;
            values[j++].logtimeGenMaxCreate = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].ulRepairCount++;
            values[j++].logtimeRepair = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].ulRepairCountOld++;
            values[j++].ulECCFixSuccess++;
            values[j++].logtimeECCFixSuccess = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].ulECCFixSuccessOld++;
            values[j++].ulECCFixFail++;
            values[j++].logtimeECCFixFail = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].ulECCFixFailOld++;
            values[j++].ulBadChecksum++;
            values[j++].logtimeBadChecksum = new JET_LOGTIME(DateTime.UtcNow);
            values[j++].ulBadChecksumOld++;
            values[j++].genCommitted++;
            values[j++].bkinfoCopyPrev = new JET_BKINFO();
            values[j++].bkinfoDiffPrev = new JET_BKINFO();
            values[j++] = new JET_DBINFOMISC();
            Debug.Assert(j == values.Length, "Not all members of values were changed", j.ToString());

            VerifyAll(values);
        }

        /// <summary>
        /// Check that JET_COLUMNBASE objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNBASE objects can be compared for equality")]
        public void VerifyJetColumnbaseEquality()
        {
            var x = new JET_COLUMNBASE
            {
                cbMax = 1,
                coltyp = JET_coltyp.Bit,
                columnid = new JET_COLUMNID { Value = 2 },
                cp = JET_CP.ASCII,
                grbit = ColumndefGrbit.ColumnFixed,
                szBaseColumnName = "foo",
                szBaseTableName = "bar"
            };
            var y = new JET_COLUMNBASE
            {
                cbMax = 1,
                coltyp = JET_coltyp.Bit,
                columnid = new JET_COLUMNID { Value = 2 },
                cp = JET_CP.ASCII,
                grbit = ColumndefGrbit.ColumnFixed,
                szBaseColumnName = "foo",
                szBaseTableName = "bar"
            };
            TestEquals(x, y);

            // This is a reference class. Operator == and != still do reference comparisons.
        }

        /// <summary>
        /// Check that JET_COLUMNBASE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNBASE objects can be compared for inequality")]
        public void VerifyJetColumnbaseInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var infos = new JET_COLUMNBASE[8];
            for (int i = 0; i < infos.Length; ++i)
            {
                infos[i] = new JET_COLUMNBASE
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.ColumnFixed,
                    szBaseColumnName = "foo",
                    szBaseTableName = "bar"
                };
            }

            int j = 1;
            infos[j++].cbMax++;
            infos[j++].coltyp = JET_coltyp.UnsignedByte;
            infos[j++].columnid = new JET_COLUMNID { Value = 101 };
            infos[j++].cp = JET_CP.Unicode;
            infos[j++].grbit |= ColumndefGrbit.ColumnNotNULL;
            infos[j++].szBaseColumnName += "baz";
            infos[j++].szBaseTableName += "baz";
            Debug.Assert(j == infos.Length, "Didn't fill in all members", infos.Length.ToString());

            VerifyAll(infos);
        }

        /// <summary>
        /// Check that IndexSegment structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that IndexSegment structures can be compared for equality")]
        public void VerifyIndexSegmentEquality()
        {
            var x = new IndexSegment("column", JET_coltyp.Currency, true, true);
            var y = new IndexSegment("column", JET_coltyp.Currency, true, true);
            TestEquals(x, y);
        }

        /// <summary>
        /// Check that IndexSegment structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that IndexSegment structures can be compared for inequality")]
        public void VerifyIndexSegmentInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var segments = new[]
            {
                new IndexSegment("column", JET_coltyp.Currency, true, true),
                new IndexSegment("column", JET_coltyp.Currency, true, false),
                new IndexSegment("column", JET_coltyp.Currency, false, true),
                new IndexSegment("column", JET_coltyp.IEEESingle, true, true),
                new IndexSegment("column2", JET_coltyp.Currency, true, true),
            };
            VerifyAll(segments);
        }

        /// <summary>
        /// Helper method to compare two equal objects.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="x">The first object.</param>
        /// <param name="y">The second object.</param>
        private static void TestEquals<T>(T x, T y) where T : IEquatable<T> 
        {
            Assert.IsTrue(x.Equals(x));
            Assert.IsTrue(y.Equals(y));

            Assert.IsTrue(x.Equals(y));
            Assert.IsTrue(y.Equals(x));

            Assert.AreEqual(x.GetHashCode(), y.GetHashCode());
            Assert.AreEqual(x.ToString(), y.ToString());

            object objA = x;
            object objB = y;
            Assert.IsTrue(objA.Equals(objB));
            Assert.IsTrue(objB.Equals(objA));
        }

        /// <summary>
        /// Helper method to compare two unequal objects.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="x">The first object.</param>
        /// <param name="y">The second object.</param>
        private static void TestNotEquals<T>(T x, T y) where T : IEquatable<T>
        {
            Assert.IsFalse(x.Equals(y));
            Assert.IsFalse(y.Equals(x));

            Assert.AreNotEqual(x.GetHashCode(), y.GetHashCode(), "{0} and {1} have the same hash code", x, y);

            object objA = x;
            object objB = y;
            Assert.IsFalse(objA.Equals(objB));
            Assert.IsFalse(objB.Equals(objA));
            Assert.IsFalse(objA.Equals(null));
            Assert.IsFalse(objB.Equals(null));
            Assert.IsFalse(objA.Equals(Any.String));
            Assert.IsFalse(objB.Equals(Any.String));
        }

        /// <summary>
        /// Verify that all objects in the collection are not equal to each other.
        /// </summary>
        /// <remarks>
        /// This method doesn't test operator == or operator != so it should be 
        /// used for reference classes, which don't normally provide those operators.
        /// </remarks>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="values">Collection of distinct objects.</param>
        private static void VerifyAll<T>(IList<T> values) where T : class, IEquatable<T>
        {
            for (int i = 0; i < values.Count - 1; ++i)
            {
                TestEquals(values[i], values[i]);
                for (int j = i + 1; j < values.Count; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same values");
                    try
                    {
                        TestNotEquals(values[i], values[j]);

                        // Only this method has the 'class' constraint so we compare against null here.
                        Assert.IsFalse(values[i].Equals(null));
                        Assert.IsFalse(values[j].Equals(null));
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Error comparing {0} and {1}", i, j);
                        throw;
                    }
                }
            }           
        }

        /// <summary>
        /// Create a new JET_DBINFOMISC object. The same values are used each time.
        /// </summary>
        /// <returns>A new JET_DBINFOMISC object.</returns>
        private static JET_DBINFOMISC CreateJetDbinfoMisc()
        {
            var epoch = new DateTime(2000, 1, 2, 3, 4, 10);
            ushort i = 789;

            Func<JET_BKINFO> bkinfo = () => new JET_BKINFO
            {
                bklogtimeMark = new JET_BKLOGTIME(epoch + TimeSpan.FromSeconds(++i), false),
                genHigh = ++i,
                genLow = ++i,
                lgposMark = new JET_LGPOS { ib = ++i, isec = ++i, lGeneration = ++i },
            };

            var native = new NATIVE_DBINFOMISC4
            {
                dbinfo = new NATIVE_DBINFOMISC
                {
                    ulVersion = ++i,
                    ulUpdate = ++i,
                    signDb = new NATIVE_SIGNATURE
                    {
                        logtimeCreate = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                        ulRandom = ++i,
                    },
                    dbstate = (int)JET_dbstate.DirtyShutdown,
                    lgposConsistent = new JET_LGPOS { ib = ++i, isec = ++i, lGeneration = ++i },
                    logtimeConsistent = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                    logtimeAttach = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                    lgposAttach = new JET_LGPOS { ib = ++i, isec = ++i, lGeneration = ++i },
                    logtimeDetach = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                    lgposDetach = new JET_LGPOS { ib = ++i, isec = ++i, lGeneration = ++i },
                    signLog = new NATIVE_SIGNATURE
                    {
                        logtimeCreate = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                        ulRandom = ++i,
                    },
                    bkinfoFullPrev = bkinfo(),
                    bkinfoIncPrev = bkinfo(),
                    bkinfoFullCur = bkinfo(),
                    fShadowingDisabled = ++i,
                    fUpgradeDb = ++i,
                    dwMajorVersion = ++i,
                    dwMinorVersion = ++i,
                    dwBuildNumber = ++i,
                    lSPNumber = ++i,
                    cbPageSize = ++i,
                },
                genMinRequired = ++i,
                genMaxRequired = ++i,
                logtimeGenMaxCreate = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                ulRepairCount = ++i,
                logtimeRepair = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                ulRepairCountOld = ++i,
                ulECCFixSuccess = ++i,
                logtimeECCFixSuccess = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                ulECCFixSuccessOld = ++i,
                ulECCFixFail = ++i,
                logtimeECCFixFail = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                ulECCFixFailOld = ++i,
                ulBadChecksum = ++i,
                logtimeBadChecksum = new JET_LOGTIME(epoch + TimeSpan.FromSeconds(++i)),
                ulBadChecksumOld = ++i,
                genCommitted = ++i,
                bkinfoCopyPrev = bkinfo(),
                bkinfoDiffPrev = bkinfo(),
            };

            var managed = new JET_DBINFOMISC();
            managed.SetFromNativeDbinfoMisc(ref native);
            return managed;
        }
    }
}
