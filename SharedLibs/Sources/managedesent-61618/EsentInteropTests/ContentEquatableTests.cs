//-----------------------------------------------------------------------
// <copyright file="ContentEquatableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for classes that implement IContentEquals and IDeepCloneable
    /// </summary>
    [TestClass]
    public partial class ContentEquatableTests
    {
        /// <summary>
        /// Check that JET_CONDITIONALCOLUMN objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_CONDITIONALCOLUMN objects can be compared for equality")]
        public void VerifyJetConditionalColumnEquality()
        {
            var x = new JET_CONDITIONALCOLUMN { szColumnName = "Column", grbit = ConditionalColumnGrbit.ColumnMustBeNonNull };
            var y = new JET_CONDITIONALCOLUMN { szColumnName = "Column", grbit = ConditionalColumnGrbit.ColumnMustBeNonNull };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_CONDITIONALCOLUMN objects can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_CONDITIONALCOLUMN objects can be compared for inequality")]
        public void VerifyJetConditionalColumnInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var conditionalcolumns = new[]
            {
                new JET_CONDITIONALCOLUMN { szColumnName = "Column", grbit = ConditionalColumnGrbit.ColumnMustBeNonNull },
                new JET_CONDITIONALCOLUMN { szColumnName = "Column", grbit = ConditionalColumnGrbit.ColumnMustBeNull },
                new JET_CONDITIONALCOLUMN { szColumnName = "Column2", grbit = ConditionalColumnGrbit.ColumnMustBeNonNull },
                new JET_CONDITIONALCOLUMN { szColumnName = null, grbit = ConditionalColumnGrbit.ColumnMustBeNonNull },
            };

            VerifyAll(conditionalcolumns);
        }

        /// <summary>
        /// Check that JET_UNICODEINDEX objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_UNICODEINDEX objects can be compared for equality")]
        public void VerifyJetUnicodeIndexEquality()
        {
            var x = new JET_UNICODEINDEX { lcid = 1033, dwMapFlags = 1 };
            var y = new JET_UNICODEINDEX { lcid = 1033, dwMapFlags = 1 };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_UNICODEINDEX structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_UNICODEINDEX objects can be compared for inequality")]
        public void VerifyJetUnicodeIndexInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var unicodeindexes = new[]
            {
                new JET_UNICODEINDEX { lcid = 1033, dwMapFlags = 1 },
                new JET_UNICODEINDEX { lcid = 1033, dwMapFlags = 2 },
                new JET_UNICODEINDEX { lcid = 1034, dwMapFlags = 1 },
            };

            VerifyAll(unicodeindexes);
        }

        /// <summary>
        /// Check that JET_RECPOS objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RECPOS objects can be compared for equality")]
        public void VerifyJetRecposEquality()
        {
            var x = new JET_RECPOS { centriesLT = 1, centriesTotal = 2 };
            var y = new JET_RECPOS { centriesLT = 1, centriesTotal = 2 };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_RECPOS structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RECPOS objects can be compared for inequality")]
        public void VerifyJetRecposInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var positions = new[]
            {
                new JET_RECPOS { centriesLT = 1, centriesTotal = 2 },
                new JET_RECPOS { centriesLT = 1, centriesTotal = 3 },
                new JET_RECPOS { centriesLT = 2, centriesTotal = 2 },
            };

            VerifyAll(positions);
        }

        /// <summary>
        /// Check that JET_INDEXCREATE objects can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXCREATE objects can be compared for equality")]
        public void VerifyJetIndexcreateEquality()
        {
            var x = new JET_INDEXCREATE
            {
                cbKey = 6,
                szKey = "-C1\0\0",
                szIndexName = "Index",
                cConditionalColumn = 1,
                rgconditionalcolumn = new[] { new JET_CONDITIONALCOLUMN { grbit = ConditionalColumnGrbit.ColumnMustBeNonNull, szColumnName = "a" } }
            };
            var y = new JET_INDEXCREATE
            {
                cbKey = 6,
                szKey = "-C1\0\0",
                szIndexName = "Index",
                cConditionalColumn = 1,
                rgconditionalcolumn = new[]
                {
                    new JET_CONDITIONALCOLUMN { grbit = ConditionalColumnGrbit.ColumnMustBeNonNull, szColumnName = "a" },
                    new JET_CONDITIONALCOLUMN { grbit = ConditionalColumnGrbit.ColumnMustBeNonNull, szColumnName = "b" },
                }
            };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_INDEXCREATE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXCREATE objects can be compared for inequality")]
        public void VerifyJetIndexCreateInequality()
        {
            // create an array of indexcreate objects
            var indexcreates = new JET_INDEXCREATE[13];

            // First make them all the same
            // (different objects with the same values)
            for (int i = 0; i < indexcreates.Length; ++i)
            {
                indexcreates[i] = new JET_INDEXCREATE
                {
                    cbKey = 6,
                    cbKeyMost = 300,
                    cbVarSegMac = 100,
                    cConditionalColumn = 2,
                    err = JET_err.Success,
                    grbit = CreateIndexGrbit.None,
                    pidxUnicode = new JET_UNICODEINDEX { dwMapFlags = 0x1, lcid = 100 },
                    rgconditionalcolumn = new[]
                    {
                        new JET_CONDITIONALCOLUMN { grbit = ConditionalColumnGrbit.ColumnMustBeNonNull, szColumnName = "a" },
                        new JET_CONDITIONALCOLUMN { grbit = ConditionalColumnGrbit.ColumnMustBeNull, szColumnName = "b" },
                        null,
                    },
                    szIndexName = "index",
                    szKey = "+foo\0\0",
                };
            }

            // Now make them all different
            int j = 1;
            indexcreates[j].rgconditionalcolumn = null; // When rgconditionalcolumn is null, cConditionalColumn must be 0.
            indexcreates[j++].cConditionalColumn = 0;
            indexcreates[j++].cbKey--;
            indexcreates[j++].cbKeyMost--;
            indexcreates[j++].cbVarSegMac--;
            indexcreates[j++].cConditionalColumn--;
            indexcreates[j++].err = JET_err.VersionStoreOutOfMemory;
            indexcreates[j++].grbit = CreateIndexGrbit.IndexUnique;
            indexcreates[j++].pidxUnicode = new JET_UNICODEINDEX { dwMapFlags = 0x2, lcid = 100 };
            indexcreates[j++].pidxUnicode = null;
            indexcreates[j++].rgconditionalcolumn[0].szColumnName = "c";
            indexcreates[j++].szIndexName = "index2";
            indexcreates[j++].szKey = "+bar\0\0";
            Debug.Assert(j == indexcreates.Length, "Too many indexcreates in array");

            // Finally compare them
            VerifyAll(indexcreates);
        }

        /// <summary>
        /// Check that JET_INDEXRANGE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXRANGE structures can be compared for equality")]
        public void VerifyJetIndexrangeEquality()
        {
            var x = new JET_INDEXRANGE { grbit = IndexRangeGrbit.RecordInIndex };
            var y = new JET_INDEXRANGE { grbit = IndexRangeGrbit.RecordInIndex };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_INDEXRANGE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_INDEXRANGE structures can be compared for inequality")]
        public void VerifyJetIndexrangeInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var ranges = new[]
            {
                new JET_INDEXRANGE { tableid = new JET_TABLEID { Value = new IntPtr(1) }, grbit = IndexRangeGrbit.RecordInIndex },
                new JET_INDEXRANGE { tableid = new JET_TABLEID { Value = new IntPtr(1) }, grbit = (IndexRangeGrbit)49 },
                new JET_INDEXRANGE { tableid = new JET_TABLEID { Value = new IntPtr(2) }, grbit = IndexRangeGrbit.RecordInIndex },
            };
            VerifyAll(ranges);
        }

        /// <summary>
        /// Check that JET_COLUMNDEF structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNDEF structures can be compared for equality")]
        public void VerifyJetColumndefEquality()
        {
            var x = new JET_COLUMNDEF
            {
                cbMax = 1,
                coltyp = JET_coltyp.Bit,
                columnid = new JET_COLUMNID { Value = 1 },
                cp = JET_CP.ASCII,
                grbit = ColumndefGrbit.None
            };
            var y = new JET_COLUMNDEF
            {
                cbMax = 1,
                coltyp = JET_coltyp.Bit,
                columnid = new JET_COLUMNID { Value = 1 },
                cp = JET_CP.ASCII,
                grbit = ColumndefGrbit.None
            };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_COLUMNDEF structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNDEF structures can be compared for inequality")]
        public void VerifyJetColumndefInequality()
        {
            // None of these objects are equal, most differ in only one member from the
            // first object. We will compare them all against each other.
            var positions = new[]
            {
                new JET_COLUMNDEF
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.None
                },
                new JET_COLUMNDEF
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.ColumnFixed
                },
                new JET_COLUMNDEF
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.Unicode,
                    grbit = ColumndefGrbit.None
                },
                new JET_COLUMNDEF
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 3 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.None
                },
                new JET_COLUMNDEF
                {
                    cbMax = 1,
                    coltyp = JET_coltyp.UnsignedByte,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.None
                },
                new JET_COLUMNDEF
                {
                    cbMax = 2,
                    coltyp = JET_coltyp.Bit,
                    columnid = new JET_COLUMNID { Value = 2 },
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.None
                },
            };
            VerifyAll(positions);
        }

        /// <summary>
        /// Check that JET_SETCOLUMN structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SETCOLUMN structures can be compared for equality")]
        public void VerifyJetSetcolumnEquality()
        {
            var x = new JET_SETCOLUMN { cbData = 4, pvData = BitConverter.GetBytes(99) };
            var y = new JET_SETCOLUMN { cbData = 4, pvData = BitConverter.GetBytes(99) };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_SETCOLUMN structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SETCOLUMN structures can be compared for inequality")]
        public void VerifyJetSetcolumnInequality()
        {
            var setcolumns = new JET_SETCOLUMN[10];
            for (int i = 0; i < setcolumns.Length; ++i)
            {
                setcolumns[i] = new JET_SETCOLUMN
                {
                    cbData = 6,
                    columnid = new JET_COLUMNID { Value = 1 },
                    err = JET_wrn.Success,
                    grbit = SetColumnGrbit.None,
                    ibData = 0,
                    ibLongValue = 0,
                    itagSequence = 1,
                    pvData = BitConverter.GetBytes(0xBADF00DL)
                };
            }

            int j = 1;
            setcolumns[j++].cbData++;
            setcolumns[j++].columnid = JET_COLUMNID.Nil;
            setcolumns[j++].err = JET_wrn.ColumnTruncated;
            setcolumns[j++].grbit = SetColumnGrbit.UniqueMultiValues;
            setcolumns[j++].ibData++;
            setcolumns[j++].ibLongValue++;
            setcolumns[j++].itagSequence++;
            setcolumns[j++].pvData = BitConverter.GetBytes(1L);
            setcolumns[j++] = new JET_SETCOLUMN();
            Debug.Assert(j == setcolumns.Length, "Didn't fill in all entries of setcolumns");
            VerifyAll(setcolumns);
        }

        /// <summary>
        /// Check that JET_RETINFO structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RETINFO structures can be compared for equality")]
        public void VerifyJetRetinfoEquality()
        {
            var x = new JET_RETINFO { ibLongValue = 1, itagSequence = 2, columnidNextTagged = new JET_COLUMNID { Value = 3U } };
            var y = new JET_RETINFO { ibLongValue = 1, itagSequence = 2, columnidNextTagged = new JET_COLUMNID { Value = 3U } };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_RETINFO structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RETINFO structures can be compared for inequality")]
        public void VerifyJetRetinfoInequality()
        {
            var retinfos = new[]
            {
                new JET_RETINFO { ibLongValue = 1, itagSequence = 2, columnidNextTagged = new JET_COLUMNID { Value = 3U } },
                new JET_RETINFO { ibLongValue = 1, itagSequence = 2, columnidNextTagged = new JET_COLUMNID { Value = 9U } },
                new JET_RETINFO { ibLongValue = 1, itagSequence = 9, columnidNextTagged = new JET_COLUMNID { Value = 3U } },
                new JET_RETINFO { ibLongValue = 9, itagSequence = 2, columnidNextTagged = new JET_COLUMNID { Value = 3U } },
            };
            VerifyAll(retinfos);
        }

        /// <summary>
        /// Check that JET_SETINFO structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SETINFO structures can be compared for equality")]
        public void VerifyJetSetinfoEquality()
        {
            var x = new JET_SETINFO { ibLongValue = 1, itagSequence = 2 };
            var y = new JET_SETINFO { ibLongValue = 1, itagSequence = 2 };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_SETINFO structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SETINFO structures can be compared for inequality")]
        public void VerifyJetSetinfoInequality()
        {
            var setinfos = new[]
            {
                new JET_SETINFO { ibLongValue = 1, itagSequence = 2 },
                new JET_SETINFO { ibLongValue = 1, itagSequence = 9 },
                new JET_SETINFO { ibLongValue = 9, itagSequence = 2 },
            };
            VerifyAll(setinfos);
        }

        /// <summary>
        /// Check that JET_COLUMNCREATE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNCREATE structures can be compared for equality")]
        public void VerifyJetColumncreateEquality()
        {
            var x = new JET_COLUMNCREATE
            {
                szColumnName = "column9",
                coltyp = JET_coltyp.Binary,
                cbMax = 0x42,
                grbit = ColumndefGrbit.ColumnAutoincrement,
                pvDefault = BitConverter.GetBytes(253),
                cbDefault = 4,
                cp = JET_CP.Unicode,
                columnid = new JET_COLUMNID
                {
                    Value = 7
                },
                err = JET_err.RecoveredWithoutUndo,
            };

            var y = new JET_COLUMNCREATE
            {
                szColumnName = "column9",
                coltyp = JET_coltyp.Binary,
                cbMax = 0x42,
                grbit = ColumndefGrbit.ColumnAutoincrement,
                pvDefault = BitConverter.GetBytes(253),
                cbDefault = 4,
                cp = JET_CP.Unicode,
                columnid = new JET_COLUMNID
                {
                    Value = 7
                },
                err = JET_err.RecoveredWithoutUndo,
            };

            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_COLUMNCREATE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNCREATE structures can be compared for inequality")]
        public void VerifyJetColumncreateInequality()
        {
            var columncreates = new JET_COLUMNCREATE[10];
            for (int i = 0; i < columncreates.Length; ++i)
            {
                columncreates[i] = new JET_COLUMNCREATE
                {
                    szColumnName = "column9",
                    coltyp = JET_coltyp.Binary,
                    cbMax = 0x42,
                    grbit = ColumndefGrbit.ColumnAutoincrement,
                    pvDefault = BitConverter.GetBytes(253),
                    cbDefault = 4,
                    cp = JET_CP.Unicode,
                    columnid = new JET_COLUMNID
                    {
                        Value = 7
                    },
                    err = JET_err.RecoveredWithoutUndo,
                };
            }

            int j = 1;
            columncreates[j++].szColumnName = "different";
            columncreates[j++].coltyp = JET_coltyp.LongBinary;
            columncreates[j++].grbit = ColumndefGrbit.ColumnEscrowUpdate;
            columncreates[j++].pvDefault = BitConverter.GetBytes(254);
            columncreates[j++].cbDefault--;
            columncreates[j++].cp = JET_CP.ASCII;
            columncreates[j++].columnid = new JET_COLUMNID
            {
                Value = 8
            };
            columncreates[j++].err = JET_err.UnicodeNormalizationNotSupported;
            columncreates[j++] = new JET_COLUMNCREATE { szColumnName = "another" };
            Debug.Assert(j == columncreates.Length, "Didn't fill in all entries of columncreates");
            VerifyAll(columncreates);
        }

        /// <summary>
        /// Check that JET_TABLECREATE structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_TABLECREATE structures can be compared for equality")]
        public void VerifyJetTablecreateEquality()
        {
            var columncreatesX = new[]
            {
                new JET_COLUMNCREATE
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1NameX = "firstIndex";
            const string Index1DescriptionX = "+col1_short\0-col2_longtext\0";

            const string Index2NameX = "secondIndex";
            const string Index2DescriptionX = "+col2_longtext\0-col1_short\0";

            var spacehintsIndexX = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsSeqX = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsLvX = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreatesX = new[]
            {
                new JET_INDEXCREATE
                {
                    szIndexName = Index1NameX,
                    szKey = Index1DescriptionX,
                    cbKey = Index1DescriptionX.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                    pSpaceHints = spacehintsIndexX,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2NameX,
                    szKey = Index2DescriptionX,
                    cbKey = Index2DescriptionX.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            JET_TABLEID tableidTemp = new JET_TABLEID()
            {
                Value = (IntPtr)2,
            };

            var tablecreateX = new JET_TABLECREATE
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreatesX.Length,
                rgcolumncreate = columncreatesX,
                rgindexcreate = indexcreatesX,
                cIndexes = indexcreatesX.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.NoFixedVarColumnsInDerivedTables,
                pSeqSpacehints = spacehintsSeqX,
                pLVSpacehints = spacehintsLvX,
                tableid = tableidTemp,
                cCreated = 7,
            };

            var columncreatesY = new[]
            {
                new JET_COLUMNCREATE
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
                new JET_COLUMNCREATE
                {
                    szColumnName = "col3_ignored",
                },
            };

            const string Index1NameY = "firstIndex";
            const string Index1DescriptionY = "+col1_short\0-col2_longtext\0";

            const string Index2NameY = "secondIndex";
            const string Index2DescriptionY = "+col2_longtext\0-col1_short\0";

            var spacehintsIndexY = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsSeqY = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsLvY = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreatesY = new[]
            {
                new JET_INDEXCREATE
                {
                    szIndexName = Index1NameY,
                    szKey = Index1DescriptionY,
                    cbKey = Index1DescriptionY.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                    pSpaceHints = spacehintsIndexY,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2NameY,
                    szKey = Index2DescriptionY,
                    cbKey = Index2DescriptionY.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
                null,
            };

            var tablecreateY = new JET_TABLECREATE()
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreatesX.Length,
                rgcolumncreate = columncreatesY,
                rgindexcreate = indexcreatesY,
                cIndexes = indexcreatesX.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.NoFixedVarColumnsInDerivedTables,
                pSeqSpacehints = spacehintsSeqY,
                pLVSpacehints = spacehintsLvY,
                tableid = tableidTemp,
                cCreated = 7,
            };

            TestContentEquals(tablecreateX, tablecreateY);
        }

        /// <summary>
        /// Check that JET_TABLECREATE structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_TABLECREATE structures can be compared for inequality")]
        public void VerifyJetTablecreateInequality()
        {
            var columncreates = new[]
            {
                new JET_COLUMNCREATE
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE
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

            var spacehintsIndex = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintHotpointSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsSeq = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsLv = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanBackward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreates = new[]
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
                null,
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            JET_TABLEID tableidTemp = new JET_TABLEID()
            {
                Value = (IntPtr)2,
            };

            var tablecreates = new JET_TABLECREATE[21];
            for (int i = 0; i < tablecreates.Length; ++i)
            {
                tablecreates[i] = new JET_TABLECREATE
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
                    grbit = CreateTableColumnIndexGrbit.NoFixedVarColumnsInDerivedTables,
                    pSeqSpacehints = spacehintsSeq,
                    pLVSpacehints = spacehintsLv,
                    tableid = tableidTemp,
                    cCreated = 7,
                };
            }

            int j = 1;
            tablecreates[j++].szTableName = "different";
            tablecreates[j++].ulPages = 57;
            tablecreates[j++].ulDensity = 98;
            tablecreates[j++].cColumns = 1;
            tablecreates[j++].rgcolumncreate = new[]
            {
                null,
                columncreates[0],
            };
            tablecreates[j].rgcolumncreate = null;
            tablecreates[j++].cColumns = 0;
            tablecreates[j++].cIndexes--;
            tablecreates[j++].cbSeparateLV = 24;
            tablecreates[j++].rgindexcreate = new[]
            {
                indexcreates[1],
                indexcreates[0],
                indexcreates[0],
            };
            tablecreates[j++].rgindexcreate = new[]
            {
                indexcreates[1],
                null,
                indexcreates[0],
            };
            tablecreates[j].rgindexcreate = null;
            tablecreates[j++].cIndexes = 0;
            tablecreates[j++].cbtyp = JET_cbtyp.AfterInsert;
            tablecreates[j++].grbit = CreateTableColumnIndexGrbit.FixedDDL;
            tablecreates[j++].pSeqSpacehints = spacehintsLv;
            tablecreates[j++].pSeqSpacehints = null;
            tablecreates[j++].pLVSpacehints = spacehintsSeq;
            tablecreates[j++].pLVSpacehints = null;
            tableidTemp.Value = new IntPtr(63);
            tablecreates[j++].tableid = tableidTemp;
            tablecreates[j++].cCreated--;
            tablecreates[j++] = new JET_TABLECREATE();
            Debug.Assert(j == tablecreates.Length, "Didn't fill in all entries of tablecreates");
            VerifyAll(tablecreates);
        }

        /// <summary>
        /// Check that JET_SPACEHINTS structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SPACEHINTS structures can be compared for equality")]
        public void VerifyJetSpaceHintsEquality()
        {
            var x = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var y = new JET_SPACEHINTS
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_SPACEHINTS structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_SPACEHINTS structures can be compared for inequality")]
        public void VerifyJetSpaceHintsInequality()
        {
            var spacehints = new JET_SPACEHINTS[9];
            for (int i = 0; i < spacehints.Length; ++i)
            {
                spacehints[i] = new JET_SPACEHINTS
                {
                    ulInitialDensity = 33,
                    cbInitial = 4096,
                    grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                    ulMaintDensity = 44,
                    ulGrowth = 144,
                    cbMinExtent = 1024 * 1024,
                    cbMaxExtent = 3 * 1024 * 1024,
                };
            }

            int j = 1;
            spacehints[j++].ulInitialDensity = 35;
            spacehints[j++].cbInitial = 2048;
            spacehints[j++].grbit = SpaceHintsGrbit.DeleteHintTableSequential;
            spacehints[j++].ulMaintDensity = 79;
            spacehints[j++].ulGrowth = 288;
            spacehints[j++].cbMinExtent = 3 * 1024 * 1024;
            spacehints[j++].cbMaxExtent = 2 * 1024 * 1024;
            spacehints[j++] = new JET_SPACEHINTS();

            Debug.Assert(j == spacehints.Length, "Didn't fill in all entries of spacehints");
            VerifyAll(spacehints);
        }

        /// <summary>
        /// Check that JET_RSTMAP structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RSTMAP structures can be compared for equality")]
        public void VerifyJetRstmapEquality()
        {
            var x = new JET_RSTMAP { szDatabaseName = "foo.edb", szNewDatabaseName = "bar.edb" };
            var y = new JET_RSTMAP { szDatabaseName = "foo.edb", szNewDatabaseName = "bar.edb" };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_RSTMAP structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RSTMAP structures can be compared for inequality")]
        public void VerifyJetRstmapInequality()
        {
            var values = new[]
            {
                new JET_RSTMAP { szDatabaseName = "foo.edb", szNewDatabaseName = "bar.edb" },
                new JET_RSTMAP { szDatabaseName = "foo.edb", szNewDatabaseName = null },
                new JET_RSTMAP { szDatabaseName = "foo.edb", szNewDatabaseName = "baz.edb" },
                new JET_RSTMAP { szDatabaseName = null, szNewDatabaseName = "bar.edb" },
                new JET_RSTMAP { szDatabaseName = "baz.edb", szNewDatabaseName = "bar.edb" },
                new JET_RSTMAP(),
            };
            VerifyAll(values);
        }

        /// <summary>
        /// Check that JET_RSTINFO structures can be
        /// compared for equality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RSTINFO structures can be compared for equality")]
        public void VerifyJetRstinfoEquality()
        {
            DateTime now = DateTime.Now;
            JET_PFNSTATUS status = (sesid, snp, snt, data) => JET_err.Success;
            var x = new JET_RSTINFO
            {
                crstmap = 1,
                lgposStop = new JET_LGPOS { ib = 1, isec = 2, lGeneration = 3 },
                logtimeStop = new JET_LOGTIME(now),
                pfnStatus = status,
                rgrstmap = new[] { new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "bar" } },
            };
            var y = new JET_RSTINFO
            {
                crstmap = 1,
                lgposStop = new JET_LGPOS { ib = 1, isec = 2, lGeneration = 3 },
                logtimeStop = new JET_LOGTIME(now),
                pfnStatus = status,
                rgrstmap = new[]
                {
                    new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "bar" },
                    new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "bar" }
                },
            };
            TestContentEquals(x, y);
        }

        /// <summary>
        /// Check that JET_RSTINFO structures can be
        /// compared for inequality.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_RSTINFO structures can be compared for inequality")]
        public void VerifyJetRstinfoInequality()
        {
            DateTime now = DateTime.Now;
            JET_PFNSTATUS status = (sesid, snp, snt, data) => JET_err.Success;

            var values = new JET_RSTINFO[7];
            for (int i = 0; i < values.Length; ++i)
            {
                values[i] = new JET_RSTINFO
                {
                    crstmap = 1,
                    lgposStop = new JET_LGPOS { ib = 1, isec = 2, lGeneration = 3 },
                    logtimeStop = new JET_LOGTIME(now),
                    pfnStatus = status,
                    rgrstmap = new[] { new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "bar" } },
                };
            }

            int j = 1;
            values[j++].crstmap--;
            values[j++].lgposStop = Any.Lgpos;
            values[j++].logtimeStop = Any.Logtime;
            values[j++].pfnStatus = (sesid, snp, snt, data) => JET_err.OutOfMemory;
            values[j++].rgrstmap = new[] { new JET_RSTMAP { szDatabaseName = "foo", szNewDatabaseName = "baz" } };
            values[j++] = new JET_RSTINFO();
            Debug.Assert(j == values.Length, "Didn't fill in all entries of values", values.Length.ToString());
            VerifyAll(values);
        }

        /// <summary>
        /// Make sure all reference non-string types are copied during cloning.
        /// </summary>
        /// <typeparam name="T">The type being cloned.</typeparam>
        /// <param name="obj">The object being cloned.</param>
        private static void VerifyDeepCloneClones<T>(T obj) where T : class, IDeepCloneable<T>
        {
            T clone = obj.DeepClone();
            Assert.AreNotSame(obj, clone);
            foreach (FieldInfo field in typeof(T).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public))
            {
                if (field.FieldType != typeof(string) && !field.FieldType.IsSubclassOf(typeof(System.Delegate)))
                {
                    object value = field.GetValue(obj);
                    object clonedValue = field.GetValue(clone);
                    if (null != value)
                    {
                        Assert.AreNotSame(value, clonedValue, "Field {0} was not cloned", field);
                    }
                }
            }
        }

        /// <summary>
        /// Helper method to compare two objects with equal content.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="x">The first object.</param>
        /// <param name="y">The second object.</param>
        private static void TestContentEquals<T>(T x, T y) where T : class, IContentEquatable<T>, IDeepCloneable<T>
        {
            Assert.IsTrue(x.ContentEquals(x));
            Assert.IsTrue(y.ContentEquals(y));

            Assert.IsTrue(x.ContentEquals(y));
            Assert.IsTrue(y.ContentEquals(x));

            Assert.AreEqual(x.ToString(), y.ToString());

            Assert.IsTrue(x.ContentEquals(x.DeepClone()));
            Assert.IsTrue(x.ContentEquals(y.DeepClone()));
            Assert.IsTrue(y.ContentEquals(y.DeepClone()));
            Assert.IsTrue(y.ContentEquals(x.DeepClone()));
        }

        /// <summary>
        /// Helper method to compare two objects with unequal content.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="x">The first object.</param>
        /// <param name="y">The second object.</param>
        private static void TestNotContentEquals<T>(T x, T y) where T : class, IContentEquatable<T>, IDeepCloneable<T>
        {
            Assert.IsFalse(x.ContentEquals(y), "{0} is content equal to {1}", x, y);
            Assert.IsFalse(y.ContentEquals(x), "{0} is content equal to {1}", y, x);

            Assert.IsFalse(x.ContentEquals(null));
            Assert.IsFalse(y.ContentEquals(null));
        }

        /// <summary>
        /// Verify that all objects in the collection are not content equal to each other
        /// and can be cloned.
        /// </summary>
        /// <remarks>
        /// This method doesn't test operator == or operator != so it should be 
        /// used for reference classes, which don't normally provide those operators.
        /// </remarks>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="values">Collection of distinct objects.</param>
        private static void VerifyAll<T>(IList<T> values) where T : class, IContentEquatable<T>, IDeepCloneable<T>
        {
            foreach (T obj in values)
            {
                VerifyDeepCloneClones(obj);
            }

            for (int i = 0; i < values.Count - 1; ++i)
            {
                TestContentEquals(values[i], values[i]);
                for (int j = i + 1; j < values.Count; ++j)
                {
                    Debug.Assert(i != j, "About to compare the same values");
                    TestNotContentEquals(values[i], values[j]);
                }
            }
        }
    }
}
