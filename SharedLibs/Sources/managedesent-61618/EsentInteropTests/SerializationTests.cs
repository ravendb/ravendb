//-----------------------------------------------------------------------
// <copyright file="SerializationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for serialization/deserialization of objects.
    /// </summary>
    [TestClass]
    public partial class SerializationTests
    {
        /// <summary>
        /// Verify that a JET_LOGTIME can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_LOGTIME can be serialized")]
        public void VerifyLogtimeCanBeSerialized()
        {
            var expected = new JET_LOGTIME(DateTime.Now);
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_BKLOGTIME can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_BKLOGTIME can be serialized")]
        public void VerifyBklogtimeCanBeSerialized()
        {
            var expected = new JET_BKLOGTIME(DateTime.Now, false);
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_LGPOS can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_LGPOS can be serialized")]
        public void VerifyLgposCanBeSerialized()
        {
            var expected = new JET_LGPOS { lGeneration = 13 };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_SIGNATURE can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_SIGNATURE can be serialized")]
        public void VerifySignatureCanBeSerialized()
        {
            var expected = new JET_SIGNATURE(1, DateTime.Now, "BROWNVM");
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_BKINFO can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_BKINFO can be serialized")]
        public void VerifyBkinfoCanBeSerialized()
        {
            var expected = new JET_BKINFO
            {
                bklogtimeMark = new JET_BKLOGTIME(DateTime.UtcNow, false),
                genHigh = 1,
                genLow = 2,
                lgposMark = new JET_LGPOS { ib = 7, isec = 8, lGeneration = 9 },
            };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that an IndexSegment can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an IndexSegment can be serialized")]
        public void VerifyIndexSegmentCanBeSerialized()
        {
            var expected = new IndexSegment("column", JET_coltyp.Text, false, true);
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that an IndexInfo can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an IndexInfo can be serialized")]
        public void VerifyIndexInfoCanBeSerialized()
        {
            var segments = new[] { new IndexSegment("column", JET_coltyp.Currency, true, false) };
            var expected = new IndexInfo(
                "index",
                CultureInfo.CurrentCulture,
                CompareOptions.IgnoreKanaType,
                segments,
                CreateIndexGrbit.IndexUnique,
                1,
                2,
                3);
            var actual = SerializeDeserialize(expected);
            Assert.AreNotSame(expected, actual);
            Assert.AreEqual(expected.Name, actual.Name);
            Assert.AreEqual(expected.CultureInfo, actual.CultureInfo);
            Assert.AreEqual(expected.IndexSegments[0].ColumnName, actual.IndexSegments[0].ColumnName);
        }

        /// <summary>
        /// Verify that a JET_COLUMNDEF can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_COLUMNDEF can be serialized")]
        public void VerifyColumndefCanBeSerialized()
        {
            var expected = new JET_COLUMNDEF { coltyp = JET_coltyp.IEEESingle };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that serializing a JET_COLUMNDEF clears the columnid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify serializing a JET_COLUMNDEF clears the columnid")]
        public void VerifyColumndefSerializationClearsColumnid()
        {
            var expected = new JET_COLUMNDEF { columnid = new JET_COLUMNID { Value = 0x9 } };
            var actual = SerializeDeserialize(expected);
            Assert.AreEqual(new JET_COLUMNID { Value = 0 }, actual.columnid);
        }

        /// <summary>
        /// Verify that a JET_CONDITIONALCOLUMN can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_CONDITIONALCOLUMN can be serialized")]
        public void VerifyConditionalColumnCanBeSerialized()
        {
            var expected = new JET_CONDITIONALCOLUMN { szColumnName = "column" };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_INDEXCREATE can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_INDEXCREATE can be serialized")]
        public void VerifyIndexCreateCanBeSerialized()
        {
            var expected = new JET_INDEXCREATE { szIndexName = "index", cbKey = 6, szKey = "+key\0\0" };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_COLUMNCREATE can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_COLUMNCREATE can be serialized")]
        public void VerifyColumnCreateCanBeSerialized()
        {
            var expected = new JET_COLUMNCREATE
            {
                szColumnName = "col1_short",
                coltyp = JET_coltyp.Short,
                cbMax = 2,
                pvDefault = BitConverter.GetBytes((short)37),
                cbDefault = 2,
            };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_SPACEHINTS can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_SPACEHINTS can be serialized")]
        public void VerifySpaceHintsCanBeSerialized()
        {
            var expected = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_TABLECREATE can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_TABLECREATE can be serialized")]
        public void VerifyTableCreateCanBeSerialized()
        {
            var columncreates = new[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                    pvDefault = BitConverter.GetBytes((short)37),
                    cbDefault = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1Name = "firstIndex";
            const string Index1Description = "+col1_short\0-col2_longtext\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+col2_longtext\0-col1_short\0";

            var spacehintsIndex = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsSeq = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsLv = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreates = new JET_INDEXCREATE[]
            {
                new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                    pSpaceHints = spacehintsIndex,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            var expected = new JET_TABLECREATE()
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreates.Length,
                rgcolumncreate = columncreates,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
                pSeqSpacehints = spacehintsSeq,
                pLVSpacehints = spacehintsLv,
            };

            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_RECPOS can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_RECPOS can be serialized")]
        public void VerifyRecposCanBeSerialized()
        {
            var expected = new JET_RECPOS { centriesLT = 10, centriesTotal = 11 };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_RECSIZE can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_RECSIZE can be serialized")]
        public void VerifyRecsizeCanBeSerialized()
        {
            var expected = new JET_RECSIZE { cbData = 101 };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_SNPROG can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_SNPROG can be serialized")]
        public void VerifySnprogCanBeSerialized()
        {
            var expected = new JET_SNPROG { cunitDone = 10, cunitTotal = 11 };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_UNICODEINDEX can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_UNICODEINDEX can be serialized")]
        public void VerifyUnicodeIndexCanBeSerialized()
        {
            var expected = new JET_UNICODEINDEX { lcid = 1234 };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_THREADSTATS can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_THREADSTATS can be serialized")]
        public void VerifyThreadstatsCanBeSerialized()
        {
            var expected = new JET_THREADSTATS { cbLogRecord = 946 };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_SETINFO can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_SETINFO can be serialized")]
        public void VerifySetinfoCanBeSerialized()
        {
            var expected = new JET_SETINFO { ibLongValue = 5, itagSequence = 6 };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_DBINFOMISC can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_DBINFOMISC can be serialized")]
        public void VerifyDbinfomiscCanBeSerialized()
        {
            var expected = new JET_DBINFOMISC { cbPageSize = 8192, signDb = new JET_SIGNATURE(1, DateTime.Now, "COMPUTAR!") };
            SerializeAndCompare(expected);
        }

        /// <summary>
        /// Verify that a JET_RSTMAP can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_RSTMAP can be serialized")]
        public void VerifyRstmapCanBeSerialized()
        {
            var expected = new JET_RSTMAP { szDatabaseName = "from.edb", szNewDatabaseName = "to.edb" };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Verify that a JET_RSTINFO can be serialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a JET_RSTINFO can be serialized")]
        public void VerifyRstinfoCanBeSerialized()
        {
            var expected = new JET_RSTINFO
            {
                crstmap = 1,
                lgposStop = Any.Lgpos,
                logtimeStop = Any.Logtime,
                rgrstmap = new[] { new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "bar" } },
            };
            SerializeAndCompareContent(expected);
        }

        /// <summary>
        /// Serialize an object to an in-memory stream then deserialize it
        /// and compare to the original.
        /// </summary>
        /// <typeparam name="T">The type to serialize.</typeparam>
        /// <param name="expected">The object to serialize.</param>
        private static void SerializeAndCompare<T>(T expected) where T : IEquatable<T>
        {
            T actual = SerializeDeserialize(expected);
            Assert.AreNotSame(expected, actual);
            Assert.AreEqual(expected, actual);            
        }

        /// <summary>
        /// Serialize an object to an in-memory stream then deserialize it
        /// and compare to the original.
        /// </summary>
        /// <typeparam name="T">The type to serialize.</typeparam>
        /// <param name="expected">The object to serialize.</param>
        private static void SerializeAndCompareContent<T>(T expected) where T : IContentEquatable<T>
        {
            T actual = SerializeDeserialize(expected);
            Assert.AreNotSame(expected, actual);
            Assert.IsTrue(expected.ContentEquals(actual));
        }

        /// <summary>
        /// Serialize an object to an in-memory stream then deserialize it.
        /// </summary>
        /// <typeparam name="T">The type to serialize.</typeparam>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>A deserialized copy of the object.</returns>
        private static T SerializeDeserialize<T>(T obj)
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, obj);

                stream.Position = 0;
                return (T)formatter.Deserialize(stream);
            }
        }
    }
}