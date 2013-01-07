//-----------------------------------------------------------------------
// <copyright file="ParameterCheckingTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Linq;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test for API parameter validation code
    /// </summary>
    [TestClass]
    public class ParameterCheckingTests
    {
        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private readonly JET_INSTANCE instance = JET_INSTANCE.Nil;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private readonly JET_SESID sesid = JET_SESID.Nil;

        /// <summary>
        /// The table used by the test.
        /// </summary>
        private readonly JET_TABLEID tableid = JET_TABLEID.Nil;

        /// <summary>
        /// The columnid used by the test.
        /// </summary>
        private readonly JET_COLUMNID columnid = JET_COLUMNID.Nil;

        /// <summary>
        /// Identifies the database used by the test.
        /// </summary>
        private readonly JET_DBID dbid = JET_DBID.Nil;

        #region Setup/Teardown
        /// <summary>
        /// Verifies no instances are leaked.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        #endregion

        #region System Parameter tests

        /// <summary>
        /// Check that an exception is thrown when JetGetSystemParameter gets a 
        /// negative max param value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetGetSystemParameter gets a negative max param value")]
        public void JetGetSystemParameterThrowsExceptionWhenMaxParamIsNegative()
        {
            int ignored = 0;
            string value;
            Api.JetGetSystemParameter(this.instance, this.sesid, JET_param.SystemPath, ref ignored, out value, -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSystemParameter gets a 
        /// too large max param value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Check that an exception is thrown when JetGetSystemParameter gets a too large max param value")]
        public void JetGetSystemParameterThrowsExceptionWhenMaxParamIsTooBig()
        {
            // This test only fails with the Unicode API (the overflow happens when we try
            // to multiply maxParam by sizeof(char))
            if (!EsentVersion.SupportsUnicodePaths)
            {
                throw new OverflowException();
            }

            int ignored = 0;
            string value;
            Api.JetGetSystemParameter(this.instance, this.sesid, JET_param.SystemPath, ref ignored, out value, Int32.MaxValue);
        }

        #endregion

        #region Database API

        /// <summary>
        /// Check that an exception is thrown when JetCreateDatabase gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetCreateDatabase gets a null database name")]
        public void JetCreateDatabaseThrowsExceptionWhenDatabaseNameIsNull()
        {
            JET_DBID dbid;
            Api.JetCreateDatabase(this.sesid, null, null, out dbid, CreateDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateDatabase2 gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetCreateDatabase2 gets a null database name")]
        public void JetCreateDatabase2ThrowsExceptionWhenDatabaseNameIsNull()
        {
            JET_DBID dbid;
            Api.JetCreateDatabase2(this.sesid, null, 0, out dbid, CreateDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateDatabase2 gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetCreateDatabase2 gets a negative page count")]
        public void JetCreateDatabase2ThrowsExceptionWhenPageCountIsNegative()
        {
            JET_DBID dbid;
            Api.JetCreateDatabase2(this.sesid, "foo.db", -2, out dbid, CreateDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAttachDatabase gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetAttachDatabase gets a null database name")]
        public void JetAttachDatabaseThrowsExceptionWhenDatabaseNameIsNull()
        {
            Api.JetAttachDatabase(this.sesid, null, AttachDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAttachDatabase2 gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetAttachDatabase2 gets null database name")]
        public void JetAttachDatabase2ThrowsExceptionWhenDatabaseNameIsNull()
        {
            Api.JetAttachDatabase2(this.sesid, null, 0, AttachDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAttachDatabase2 gets a 
        /// negative max page count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetAttachDatabase2 gets negative max page count")]
        public void JetAttachDatabase2ThrowsExceptionWhenMaxPagesIsNegative()
        {
            Api.JetAttachDatabase2(this.sesid, "foo.db", -1, AttachDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetOpenDatabase gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetOpenDatabase gets null database name")]
        public void JetOpenDatabaseThrowsExceptionWhenDatabaseNameIsNull()
        {
            JET_DBID dbid;
            Api.JetOpenDatabase(this.sesid, null, null, out dbid, OpenDatabaseGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGrowDatabase gets
        /// a negative page count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetGrowDatabase a negative page count")]
        public void VerifyJetGrowDatabaseThrowsExceptionWhenDesiredPagesIsNegative()
        {
            int ignored;
            Api.JetGrowDatabase(JET_SESID.Nil, JET_DBID.Nil, -1, out ignored);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetDatabaseSize gets a 
        /// null database name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetSetDatabaseSize gets null database name")]
        public void JetSetDatabaseSizeThrowsExceptionWhenDatabaseNameIsNull()
        {
            int ignored;
            Api.JetSetDatabaseSize(this.sesid, null, 0, out ignored);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetDatabaseSize gets
        /// a negative page count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetSetDatabaseSize a negative page count")]
        public void VerifyJetSetDatabaseSizeThrowsExceptionWhenDesiredPagesIsNegative()
        {
            int ignored;
            Api.JetSetDatabaseSize(this.sesid, "foo.edb", -1, out ignored);
        }

        /// <summary>
        /// JetCompact should throw an exception when
        /// the source database is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("JetCompact should throw an exception when the source database is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestJetCompactThrowsExceptionWhenSourceIsNull()
        {
            Api.JetCompact(this.sesid, null, "destination", null, null, CompactGrbit.None);
        }

        /// <summary>
        /// JetCompact should throw an exception when
        /// the source database is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("JetCompact should throw an exception when the source database is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestJetCompactThrowsExceptionWhenDestinationIsNull()
        {
            Api.JetCompact(this.sesid, "source", null, null, null, CompactGrbit.None);
        }

        /// <summary>
        /// JetCompact should throw an exception when
        /// the ignored parameter is non-null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("JetCompact should throw an exception when the ignored parameter is non-null")]
        [ExpectedException(typeof(ArgumentException))]
        public void TestJetCompactThrowsExceptionWhenIgnoredIsNonNull()
        {
#pragma warning disable 618,612 // JET_CONVERT is obsolete
            Api.JetCompact(this.sesid, "source", "destination", null, new Converter(), CompactGrbit.None);
#pragma warning restore 618,612
        }

        #endregion Database API

        #region Streaming Backup/Restore

        /// <summary>
        /// Check that an exception is thrown when JetOpenFileInstance gets a 
        /// null file name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetOpenFileInstance gets a null file name")]
        public void JetOpenFileInstanceThrowsExceptionWhenFileNameIsNull()
        {
            JET_HANDLE handle;
            long fileSizeLow;
            long fileSizeHigh;
            Api.JetOpenFileInstance(this.instance, null, out handle, out fileSizeLow, out fileSizeHigh);
        }

        /// <summary>
        /// Check that an exception is thrown when JetReadFileInstance gets a 
        /// null buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that an exception is thrown when JetReadFileInstance gets a null buffer")]
        public void JetReadFileInstanceThrowsExceptionWhenBufferIsNull()
        {
            int bytesRead;
            Api.JetReadFileInstance(this.instance, JET_HANDLE.Nil, null, 0, out bytesRead);
        }

        /// <summary>
        /// Check that an exception is thrown when JetReadFileInstance gets a 
        /// negative buffer size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetReadFileInstance gets a negative buffer size")]
        public void JetReadFileInstanceThrowsExceptionWhenBufferSizeIsNegative()
        {
            int bytesRead;
            Api.JetReadFileInstance(this.instance, JET_HANDLE.Nil, new byte[1], -1, out bytesRead);
        }

        /// <summary>
        /// Check that an exception is thrown when JetReadFileInstance gets a 
        /// buffer size that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetReadFileInstance gets a buffer size that is too long")]
        public void JetReadFileInstanceThrowsExceptionWhenBufferSizeIsTooLong()
        {
            int bytesRead;
            Api.JetReadFileInstance(this.instance, JET_HANDLE.Nil, new byte[1], 2, out bytesRead);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetAttachInfoInstance gets a buffer size that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetGetAttachInfoInstance gets a buffer size that is negative")]
        public void JetGetAttachInfoInstanceThrowsExceptionWhenMaxCharsIsNegative()
        {
            string ignored;
            int ignored2;
            Api.JetGetAttachInfoInstance(this.instance, out ignored, -1, out ignored2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetAttachInfoInstance gets a 
        /// too large max param value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Check that an exception is thrown when JetGetAttachInfoInstance gets a too large max param value")]
        public void JetGetAttachInfoInstanceThrowsExceptionWhenMaxParamIsTooBig()
        {
            // This test only fails with the Unicode API (the overflow happens when we try
            // to multiply maxParam by sizeof(char))
            if (!EsentVersion.SupportsUnicodePaths)
            {
                throw new OverflowException();
            }

            string ignored;
            int ignored2;
            Api.JetGetAttachInfoInstance(this.instance, out ignored, Int32.MaxValue, out ignored2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetLogInfoInstance gets a buffer size that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetGetLogInfoInstance gets a buffer size that is negative")]
        public void JetGetLogInfoInstanceThrowsExceptionWhenMaxCharsIsNegative()
        {
            string ignored;
            int ignored2;
            Api.JetGetLogInfoInstance(this.instance, out ignored, -1, out ignored2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetLogInfoInstance gets a 
        /// too large max param value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Check that an exception is thrown when JetGetLogInfoInstance gets a too large max param value")]
        public void JetGetLogInfoInstanceThrowsExceptionWhenMaxParamIsTooBig()
        {
            // This test only fails with the Unicode API (the overflow happens when we try
            // to multiply maxParam by sizeof(char))
            if (!EsentVersion.SupportsUnicodePaths)
            {
                throw new OverflowException();
            }

            string ignored;
            int ignored2;
            Api.JetGetLogInfoInstance(this.instance, out ignored, Int32.MaxValue, out ignored2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetTruncateLogInfoInstance gets a buffer size that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when JetGetTruncateLogInfoInstance gets a buffer size that is negative")]
        public void JetGetTruncateLogInfoInstanceThrowsExceptionWhenMaxCharsIsNegative()
        {
            string ignored;
            int ignored2;
            Api.JetGetTruncateLogInfoInstance(this.instance, out ignored, -1, out ignored2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetTruncateLogInfoInstance gets a 
        /// too large max param value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Check that an exception is thrown when JetGetTruncateLogInfoInstance gets a too large max param value")]
        public void JetGetTruncateLogInfoInstanceThrowsExceptionWhenMaxParamIsTooBig()
        {
            // This test only fails with the Unicode API (the overflow happens when we try
            // to multiply maxParam by sizeof(char))
            if (!EsentVersion.SupportsUnicodePaths)
            {
                throw new OverflowException();
            }

            string ignored;
            int ignored2;
            Api.JetGetTruncateLogInfoInstance(this.instance, out ignored, Int32.MaxValue, out ignored2);
        }

        #endregion

        #region DDL

        /// <summary>
        /// Check that an exception is thrown when JetOpenTable gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetOpenTable gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetOpenTableThrowsExceptionWhenTableNameIsNull()
        {
            JET_TABLEID ignoredTableid;
            Api.JetOpenTable(this.sesid, this.dbid, null, null, 0, OpenTableGrbit.None, out ignoredTableid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetOpenTable gets a 
        /// parameters size that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetOpenTable gets a parameters size that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetOpenTableThrowsExceptionWhenParametersSizeIsTooLong()
        {
            byte[] parameters = new byte[1];
            JET_TABLEID ignoredTableid;
            Api.JetOpenTable(this.sesid, this.dbid, "table", parameters, parameters.Length + 1, OpenTableGrbit.None, out ignoredTableid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateTable gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateTable gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetCreateTableThrowsExceptionWhenTableNameIsNull()
        {
            JET_TABLEID ignoredTableid;
            Api.JetCreateTable(this.sesid, this.dbid, null, 0, 100, out ignoredTableid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetDeleteTable gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetDeleteTable gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetDeleteTableThrowsExceptionWhenTableNameIsNull()
        {
            Api.JetDeleteTable(this.sesid, this.dbid, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetTableInfo gets a 
        /// null result buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetTableInfo gets a null result buffer")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetTableInfoThrowsExceptionWhenResultIsNull()
        {
            Api.JetGetTableInfo(this.sesid, this.tableid, null, JET_TblInfo.SpaceUsage);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetColumnInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetColumnInfo gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetColumnInfoThrowsExceptionWhenTableNameIsNull()
        {
            JET_COLUMNDEF columndef;
            Api.JetGetColumnInfo(this.sesid, this.dbid, null, "column", out columndef);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetColumnInfo gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetColumnInfo gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetColumnInfoThrowsExceptionWhenColumnNameIsNull()
        {
            JET_COLUMNDEF columndef;
            Api.JetGetColumnInfo(this.sesid, this.dbid, "table", null, out columndef);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetColumnInfo gets a 
        /// null table name.
        /// </summary>
        /// <remarks>
        /// This tests the version of the API that takes a JET_COLUMNLIST.
        /// </remarks>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetColumnInfo gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetColumnInfoThrowsExceptionWhenTableNameIsNull2()
        {
            JET_COLUMNLIST columnlist;
            Api.JetGetColumnInfo(this.sesid, this.dbid, null, null, out columnlist);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetColumnInfo gets a 
        /// null table name.
        /// </summary>
        /// <remarks>
        /// This tests the version of the API that takes a JET_COLUMNBASE.
        /// </remarks>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetColumnInfo gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetColumnInfoThrowsExceptionWhenTableNameIsNull3()
        {
            JET_COLUMNBASE columnbase;
            Api.JetGetColumnInfo(this.sesid, this.dbid, null, null, out columnbase);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetTableColumnInfo gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetTableColumnInfo gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetTableColumnInfoThrowsExceptionWhenColumnNameIsNull()
        {
            JET_COLUMNDEF columndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, null, out columndef);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetIndexInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetIndexInfo(obsolete) gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetIndexInfoObsoleteThrowsExceptionWhenTableNameIsNull()
        {
            JET_INDEXLIST indexlist;
#pragma warning disable 612,618 // Obsolete
            Api.JetGetIndexInfo(this.sesid, this.dbid, null, String.Empty, out indexlist);
#pragma warning restore 612,618
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetIndexInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetIndexInfo(ushort) gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetIndexInfoUshortThrowsExceptionWhenTableNameIsNull()
        {
            ushort result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, null, String.Empty, out result, JET_IdxInfo.Default);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetIndexInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetIndexInfo(int) gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetIndexInfoIntThrowsExceptionWhenTableNameIsNull()
        {
            int result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, null, String.Empty, out result, JET_IdxInfo.Default);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetIndexInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetIndexInfo(JET_INDEXID) gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetIndexInfoIndexidThrowsExceptionWhenTableNameIsNull()
        {
            JET_INDEXID result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, null, String.Empty, out result, JET_IdxInfo.Default);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetIndexInfo gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetIndexInfo(JET_INDEXLIST) gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGetIndexInfoIndexListThrowsExceptionWhenTableNameIsNull()
        {
            JET_INDEXLIST result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, null, String.Empty, out result, JET_IdxInfo.Default);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRenameTable gets a 
        /// null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRenameTable gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetRenameTableThrowsExceptionWhenTableNameIsNull()
        {
            Api.JetRenameTable(this.sesid, this.dbid, null, "newtable");
        }

        /// <summary>
        /// Check that an exception is thrown when JetRenameTable gets a 
        /// null new table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRenameTable gets a null new table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetRenameTableThrowsExceptionWhenNewTableNameIsNull()
        {
            Api.JetRenameTable(this.sesid, this.dbid, "oldtable", null);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetRenameColumn gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when JetRenameColumn gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetRenameColumnThrowsExceptionWhencolumnNameIsNull()
        {
            Api.JetRenameColumn(this.sesid, this.tableid, null, "newcolumn", RenameColumnGrbit.None);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetRenameColumn gets a 
        /// null new column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when JetRenameColumn gets a null new column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetRenameColumnThrowsExceptionWhenNewColumnNameIsNull()
        {
            Api.JetRenameColumn(this.sesid, this.tableid, "oldcolumn", null, RenameColumnGrbit.None);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetSetColumnDefaultValue gets a null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description(" Verify that an exception is thrown when JetSetColumnDefaultValue gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetSetColumnDefaultValueThrowsExceptionWhenTableNameIsNull()
        {
            Api.JetSetColumnDefaultValue(this.sesid, this.dbid, null, "column", new byte[1], 1, SetColumnDefaultValueGrbit.None);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetSetColumnDefaultValue gets a null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description(" Verify that an exception is thrown when JetSetColumnDefaultValue gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetSetColumnDefaultValueThrowsExceptionWhenColumnNameIsNull()
        {
            Api.JetSetColumnDefaultValue(this.sesid, this.dbid, "table", null, new byte[1], 1, SetColumnDefaultValueGrbit.None);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetSetColumnDefaultValue gets a negative data size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when JetSetColumnDefaultValue gets a negative data size")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetSetColumnDefaultValueThrowsExceptionWhenDataSizeIsNegative()
        {
            Api.JetSetColumnDefaultValue(this.sesid, this.dbid, "table", "column", null, -1, SetColumnDefaultValueGrbit.None);
        }

        /// <summary>
        /// Verify that an exception is thrown when JetSetColumnDefaultValue gets a data size that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when JetSetColumnDefaultValue gets a data size that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetSetColumnDefaultValueThrowsExceptionWhenDataSizeIsTooLong()
        {
            Api.JetSetColumnDefaultValue(this.sesid, this.dbid, "table", "column", new byte[1], 2, SetColumnDefaultValueGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAddColumn gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetAddColumn gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetAddColumnThrowsExceptionWhenColumnNameIsNull()
        {
            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Binary,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(
                this.sesid,
                this.tableid,
                null,
                columndef,
                null,
                0,
                out columnid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAddColumn gets a 
        /// null column definition.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetAddColumn gets a null column definition")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetAddColumnThrowsExceptionWhenColumndefIsNull()
        {
            JET_COLUMNID columnid;
            Api.JetAddColumn(
                this.sesid,
                this.tableid,
                "column",
                null,
                null,
                0,
                out columnid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAddColumn gets a 
        /// default value length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetAddColumn gets a default value length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetAddColumnThrowsExceptionWhenDefaultValueLengthIsNegative()
        {
            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Binary,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(
                this.sesid,
                this.tableid,
                "NegativeDefaultValue",
                columndef,
                null,
                -1,
                out columnid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAddColumn gets a 
        /// default value length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetAddColumn gets a default value length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetAddColumnThrowsExceptionWhenDefaultValueLengthIsTooLong()
        {
            var defaultValue = new byte[10];
            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Binary,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(
                this.sesid,
                this.tableid,
                "BadDefaultValue",
                columndef,
                defaultValue,
                defaultValue.Length + 1,
                out columnid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetAddColumn gets a 
        /// default value that is null with a non-zero default value size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetAddColumn gets a default value that is null with a non-zero default value size")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetAddColumnThrowsExceptionWhenDefaultValueIsUnexpectedNull()
        {
            var defaultValue = new byte[10];
            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Binary,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(
                this.sesid,
                this.tableid,
                "BadDefaultValue",
                columndef,
                null,
                1,
                out columnid);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex gets a 
        /// null name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex gets a null name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetCreateIndexThrowsExceptionWhenNameIsNull()
        {
            Api.JetCreateIndex(this.sesid, this.tableid, null, CreateIndexGrbit.None, "+foo\0", 6, 100);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex gets a 
        /// density that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex gets a density that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetCreateIndexThrowsExceptionWhenDensityIsNegative()
        {
            Api.JetCreateIndex(this.sesid, this.tableid, "BadIndex,", CreateIndexGrbit.None, "+foo\0", 6, -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex gets a 
        /// key description length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex gets a key description length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetCreateIndexThrowsExceptionWhenKeyDescriptionLengthIsNegative()
        {
            Api.JetCreateIndex(this.sesid, this.tableid, "BadIndex,", CreateIndexGrbit.None, "+foo\0", -1, 100);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex gets a 
        /// key description length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex gets a key description length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetCreateIndexThrowsExceptionWhenKeyDescriptionLengthIsTooLong()
        {
            Api.JetCreateIndex(this.sesid, this.tableid, "BadIndex,", CreateIndexGrbit.None, "+foo\0", 77, 100);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex2 gets 
        /// null indexcreates.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex2 gets null indexcreates")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetCreateIndex2ThrowsExceptionWhenIndexcreatesAreNull()
        {
            Api.JetCreateIndex2(this.sesid, this.tableid, null, 0);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex2 gets 
        /// a negative indexcreate count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex2 gets a negative indexcreate count")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetCreateIndex2ThrowsExceptionWhenNumIndexcreatesIsNegative()
        {
            var indexcreates = new[] { new JET_INDEXCREATE() };
            Api.JetCreateIndex2(this.sesid, this.tableid, indexcreates, -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex2 gets 
        /// an indexcreate count that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex2 gets an indexcreate count that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetCreateIndex2ThrowsExceptionWhenNumIndexcreatesIsTooLong()
        {
            var indexcreates = new[] { new JET_INDEXCREATE() };
            Api.JetCreateIndex2(this.sesid, this.tableid, indexcreates, indexcreates.Length + 1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetCreateIndex2 gets a 
        /// null index name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetCreateIndex2 gets a null index name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetCreateIndex2ThrowsExceptionWhenIndexNameIsNull()
        {
            const string Key = "+column\0";
            var indexcreates = new[]
            {
                new JET_INDEXCREATE
                {
                    cbKey = Key.Length,
                    szKey = Key,
                },
            };
            Api.JetCreateIndex2(this.sesid, this.tableid, indexcreates, indexcreates.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetDeleteColumn gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetDeleteColumn gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetDeleteColumnThrowsExceptionWhenColumnNameIsNull()
        {
            Api.JetDeleteColumn(this.sesid, this.tableid, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetDeleteColumn gets a 
        /// null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetDeleteColumn gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetDeleteColumn2ThrowsExceptionWhenColumnNameIsNull()
        {
            Api.JetDeleteColumn2(this.sesid, this.tableid, null, DeleteColumnGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetDeleteIndex gets a 
        /// null index name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetDeleteIndex gets a null index name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetDeleteIndexThrowsExceptionWhenIndexNameIsNull()
        {
            Api.JetDeleteIndex(this.sesid, this.tableid, null);
        }

        #endregion

        #region Meta-data Helpers

        /// <summary>
        /// Verify that an exception is thrown when TryOpenTable gets a null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when TryOpenTable gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyTryOpenTableThrowsExceptionWhenTableNameIsNull()
        {
            JET_TABLEID t;
            Api.TryOpenTable(this.sesid, this.dbid, null, OpenTableGrbit.None, out t);
        }

        /// <summary>
        /// Verify that an exception is thrown when GetTableColumnid gets a null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when GetTableColumnid gets a null column name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyGetTableColumnidThrowsExceptionWhenColumnNameIsNull()
        {
            Api.GetTableColumnid(this.sesid, this.tableid, null);
        }

        /// <summary>
        /// Verify that an exception is thrown when GetTableColumns gets a null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when GetTableColumns gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyGetTableColumnsThrowsExceptionWhenTableNameIsNull()
        {
            Api.GetTableColumns(this.sesid, this.dbid, null);
        }

        /// <summary>
        /// Verify that an exception is thrown when GetTableIndexes gets a null table name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an exception is thrown when GetTableIndexes gets a null table name")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyGetTableIndexesThrowsExceptionWhenTableNameIsNull()
        {
            Api.GetTableIndexes(this.sesid, this.dbid, null);
        }

        #endregion

        #region Temporary Table Creation

        /// <summary>
        /// Null columns is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable throws an Exception when columns parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTempTableThrowsExceptionWhenColumnsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable(this.sesid, null, 0, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Null columnids is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Verify JetOpenTempTable throws an Exception when columnids parameter is null")]
        public void VerifyJetOpenTempTableThrowsExceptionWhenColumnidsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            Api.JetOpenTempTable(this.sesid, columns, columns.Length, TempTableGrbit.None, out tableidIgnored, null);
        }

        /// <summary>
        /// Columnids must match columndefs in length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Verify JetOpenTempTable throws an Exception when columnids parameter is the wrong length")]
        public void VerifyJetOpenTempTableThrowsExceptionWhenColumnidsIsTooShort()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF(), new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[columns.Length - 1];
            Api.JetOpenTempTable(this.sesid, columns, columns.Length, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Negative column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Verify JetOpenTempTable throws an Exception when column count is negative")]
        public void VerifyJetOpenTempTableThrowsExceptionWhenColumnCountIsNegative()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable(this.sesid, columns, -1, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Too-long column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable throws an Exception when column count is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTempTableThrowsExceptionWhenColumnCountIsTooLong()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable(this.sesid, columns, 2, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Null columns is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable2 throws an Exception when columns parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTempTable2ThrowsExceptionWhenColumnsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable2(this.sesid, null, 0, 1033, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Null columnids is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable2 throws an Exception when columnids parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTempTable2ThrowsExceptionWhenColumnidsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            Api.JetOpenTempTable2(this.sesid, columns, columns.Length, 1033, TempTableGrbit.None, out tableidIgnored, null);
        }

        /// <summary>
        /// Columnids must match columndefs in length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Verify JetOpenTempTable2 throws an Exception when columnids parameter is the wrong length")]
        public void VerifyJetOpenTempTable2ThrowsExceptionWhenColumnidsIsTooShort()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF(), new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[columns.Length - 1];
            Api.JetOpenTempTable2(this.sesid, columns, columns.Length, 1033, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Negative column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable2 throws an Exception when column count is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTempTable2ThrowsExceptionWhenColumnCountIsNegative()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable2(this.sesid, columns, -1, 1033, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Too-long column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable2 throws an Exception when column count is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTempTable2ThrowsExceptionWhenColumnCountIsTooLong()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable2(this.sesid, columns, 2, 1033, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Null columns is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable3 throws an Exception when columns parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTempTable3ThrowsExceptionWhenColumnsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable3(this.sesid, null, 0, null, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Null columnids is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable3 throws an Exception when columnids parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTempTable3ThrowsExceptionWhenColumnidsIsNull()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            Api.JetOpenTempTable3(this.sesid, columns, columns.Length, null, TempTableGrbit.None, out tableidIgnored, null);
        }

        /// <summary>
        /// Columnids must match columndefs in length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Verify JetOpenTempTable3 throws an Exception when columnids parameter is the wrong length")]
        public void VerifyJetOpenTempTable3ThrowsExceptionWhenColumnidsIsTooShort()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF(), new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[columns.Length - 1];
            Api.JetOpenTempTable3(this.sesid, columns, columns.Length, null, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Negative column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable3 throws an Exception when column count is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTempTable3ThrowsExceptionWhenColumnCountIsNegative()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable3(this.sesid, columns, -1, null, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Too-long column count is invalid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTempTable3 throws an Exception when column count is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTempTable3ThrowsExceptionWhenColumnCountIsTooLong()
        {
            JET_TABLEID tableidIgnored;
            var columns = new[] { new JET_COLUMNDEF() };
            var columnids = new JET_COLUMNID[1];
            Api.JetOpenTempTable3(this.sesid, columns, 2, null, TempTableGrbit.None, out tableidIgnored, columnids);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the TemporaryTable parameter is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an exception when the TemporaryTable parameter is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenTemporaryTableIsNull()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentNullException();
            }

            VistaApi.JetOpenTemporaryTable(this.sesid, null);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the columns member is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an Exception when columns member is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenColumnsIsNull()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentNullException();
            }

            var opentemporarytable = new JET_OPENTEMPORARYTABLE
            {
                ccolumn = 1,
                prgcolumnid = new JET_COLUMNID[1],
            };
            VistaApi.JetOpenTemporaryTable(this.sesid, opentemporarytable);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the columnids member is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an Exception when columnids member is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenColumnidsIsNull()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentNullException();
            }

            var opentemporarytable = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef = new[] { new JET_COLUMNDEF() },
                ccolumn = 1,
            };
            VistaApi.JetOpenTemporaryTable(this.sesid, opentemporarytable);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the columnids member is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an Exception when columnids member isn't long enough")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenColumnidsIsTooShort()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentOutOfRangeException();
            }

            var opentemporarytable = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef = new[] { new JET_COLUMNDEF(), new JET_COLUMNDEF() },
                ccolumn = 2,
                prgcolumnid = new JET_COLUMNID[1],
            };
            VistaApi.JetOpenTemporaryTable(this.sesid, opentemporarytable);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the column count member is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an Exception when column count is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenColumnCountIsNegative()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentOutOfRangeException();
            }

            var opentemporarytable = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef = new[] { new JET_COLUMNDEF() },
                ccolumn = -1,
                prgcolumnid = new JET_COLUMNID[1],
            };
            VistaApi.JetOpenTemporaryTable(this.sesid, opentemporarytable);
        }

        /// <summary>
        /// Verify JetOpenTemporaryTable throws an exception when the column count member is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetOpenTemporaryTable throws an Exception when column count is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyJetOpenTemporaryTableThrowsExceptionWhenColumnCountIsTooLong()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                throw new ArgumentOutOfRangeException();
            }

            var opentemporarytable = new JET_OPENTEMPORARYTABLE
            {
                prgcolumndef = new[] { new JET_COLUMNDEF() },
                ccolumn = 2,
                prgcolumnid = new JET_COLUMNID[1],
            };
            VistaApi.JetOpenTemporaryTable(this.sesid, opentemporarytable);
        }

        #endregion

        #region Navigation

        /// <summary>
        /// Check that an exception is thrown when JetGotoBookmark gets a 
        /// null bookmark.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoBookmark gets a null bookmark")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGotoBookmarkThrowsExceptionWhenBookmarkIsNull()
        {
            Api.JetGotoBookmark(this.sesid, this.tableid, null, 0);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoBookmark gets a 
        /// negative bookmark length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoBookmark gets a negative bookmark length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoBookmarkThrowsExceptionWhenBookmarkLengthIsNegative()
        {
            var bookmark = new byte[1];
            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoBookmark gets a 
        /// bookmark length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoBookmark gets a bookmark length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoBookmarkThrowsExceptionWhenBookmarkLengthIsTooLong()
        {
            var bookmark = new byte[1];
            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, bookmark.Length + 1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a 
        /// null secondary key.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a null secondary key")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetGotoSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyIsNull()
        {
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                null,
                0,
                new byte[1],
                1,
                GotoSecondaryIndexBookmarkGrbit.None); 
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a 
        /// negative secondary key length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a negative secondary key length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyLengthIsNegative()
        {
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[1], 
                -1,
                new byte[1],
                1,
                GotoSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a 
        /// secondary key length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a secondary key length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyLengthIsTooLong()
        {
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[1], 
                2,
                new byte[1],
                1,
                GotoSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a 
        /// negative primary key length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a negative primary key length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoSecondaryIndexBookmarkThrowsExceptionWhenPrimaryKeyLengthIsNegative()
        {
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[1],
                1,
                new byte[1],
                -1,
                GotoSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a 
        /// primary key length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGotoSecondaryIndexBookmark gets a primary key length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGotoSecondaryIndexBookmarkThrowsExceptionWhenPrimaryKeyLengthIsTooLong()
        {
            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[1],
                1,
                new byte[1],
                2,
                GotoSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetMakeKey gets 
        /// null data and a non-null length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetMakeKey gets null data and a non-null length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetMakeKeyThrowsExceptionWhenDataIsNull()
        {
            Api.JetMakeKey(this.sesid, this.tableid, null, 2, MakeKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetMakeKey gets a 
        /// data length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetMakeKey gets a data length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetMakeKeyThrowsExceptionWhenDataLengthIsNegative()
        {
            var data = new byte[1];
            Api.JetMakeKey(this.sesid, this.tableid, data, -1, MakeKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetMakeKey gets a 
        /// data length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetMakeKey gets a data length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetMakeKeyThrowsExceptionWhenDataLengthIsTooLong()
        {
            var data = new byte[1];
            Api.JetMakeKey(this.sesid, this.tableid, data, data.Length + 1, MakeKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetIndexRecordCount gets a 
        /// negative max record count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetIndexRecordCount gets a negative max record count")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetIndexRecordCountThrowsExceptionWhenMaxRecordsIsNegative()
        {
            int numRecords;
            Api.JetIndexRecordCount(this.sesid, this.tableid, out numRecords, -1);
        }

        /// <summary>
        /// Check that an exception is thrown when passing in NULL as the 
        /// ranges to JetIntersectIndexes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when passing in NULL as the ranges to JetIntersectIndexes")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetIntersectIndexesThrowsExceptionWhenTableidsIsNull()
        {
            JET_RECORDLIST recordlist;
            Api.JetIntersectIndexes(this.sesid, null, 0, out recordlist, IntersectIndexesGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when intersecting just one index.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when intersecting just one index")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetIntersectIndexesThrowsExceptionWhenIntersectingOneTableid()
        {
            var ranges = new JET_INDEXRANGE[1];
            ranges[0] = new JET_INDEXRANGE { tableid = this.tableid };

            JET_RECORDLIST recordlist;
            Api.JetIntersectIndexes(this.sesid, ranges, 1, out recordlist, IntersectIndexesGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when IntersectIndexes gets null
        /// as the tableid argument.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when IntersectIndexes gets null as the tableid argument")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void IntersectIndexesThrowsExceptionWhenTableidIsNull()
        {
            Api.IntersectIndexes(this.sesid, null).ToArray();
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a negative count.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a negative count")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenCountIsNegative()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[1][], new int[1], -1, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets null keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets null keys")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetPrereadKeysThrowsExceptionWhenKeysIsNull()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentNullException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, null, new int[1], 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets null key lengths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets null key counts")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetPrereadKeysThrowsExceptionWhenKeyLengthsIsNull()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentNullException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[1][], null, 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a count that is longer than the keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a count that is longer than the keys")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenCountIsLongerThanKeys()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[1][], new int[2], 2, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a count that is longer than the key lengths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a count that is longer than the key lengths")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenCountIsLongerThanKeyLengths()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[2][], new int[1], 2, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a negative index.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a negative index")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexIsNegative()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[1][], new int[1], -1, 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a non-zero index and null keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a non-zero index and null keys")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexIsNonZeroAndKeysIsNull()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, null, new int[2], 1, 1, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets a non-zero index and null key lengths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets a non-zero index and null key lengths")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexIsNonZeroAndKeyLengthsIsNull()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[2][], null, 1, 1, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets an index past the end of the keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets an index past the end of the keys")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexIsPastEndOfKeys()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[1][], new int[2], 1, 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets an index past the end of the key lengths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets an index past the end of the key lengths")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexIsPastEndOfKeyLengths()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[2][], new int[1], 1, 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets an index/count past the end of the keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets an index/count past the end of the keys")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexCountIsPastEndOfKeys()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[3][], new int[4], 1, 3, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Check that an exception is thrown when JetPrereadKeys gets an index/count past the end of the key lengths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetPrereadKeys gets an index/count past the end of the key lengths")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetPrereadKeysThrowsExceptionWhenIndexCountIsPastEndOfKeyLengths()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int ignored;
            Windows7Api.JetPrereadKeys(this.sesid, this.tableid, new byte[4][], new int[3], 1, 3, out ignored, PrereadKeysGrbit.Forward);
        }

        #endregion

        #region Data Retrieval

        /// <summary>
        /// Check that an exception is thrown when JetGetBookmark gets a 
        /// null bookmark and non-null length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetBookmark gets a null bookmark and non-null length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetBookmarkThrowsExceptionWhenBookmarkIsNull()
        {
            int actualSize;
            Api.JetGetBookmark(this.sesid, this.tableid, null, 10, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetBookmark gets a 
        /// bookmark length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetBookmark gets a bookmark length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetBookmarkThrowsExceptionWhenBookmarkLengthIsNegative()
        {
            int actualSize;
            var bookmark = new byte[1];
            Api.JetGetBookmark(this.sesid, this.tableid, bookmark, -1, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetBookmark gets a 
        /// bookmark length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetGetBookmark gets a bookmark length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetBookmarkThrowsExceptionWhenBookmarkLengthIsTooLong()
        {
            int actualSize;
            var bookmark = new byte[1];
            Api.JetGetBookmark(this.sesid, this.tableid, bookmark, bookmark.Length + 1, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// null secondary key buffer and non-zero length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the secondary bookmark is null and the length is non-zero")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyIsNull()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                null,
                10,
                out ignored1,
                null,
                0,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// secondary key buffer length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the secondary bookmark length is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyLengthIsNegative()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[1], 
                -1,
                out ignored1,
                null,
                0,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// secondary key buffer length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the secondary bookmark length is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenSecondaryKeyLengthIsTooLong()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                new byte[2], 
                3,
                out ignored1,
                null,
                0,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// null primary key buffer and non-zero length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the primary bookmark is null and the length is non-zero")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenPrimaryKeyIsNull()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                null,
                0,
                out ignored1,
                null,
                1,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// primary key buffer length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the primary bookmark length is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenPrimaryKeyLengthIsNegative()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                null,
                0,
                out ignored1,
                new byte[1],
                -1,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetGetSecondaryIndexBookmark gets a 
        /// primary key buffer length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JetGetSecondaryIndexBookmark throws an exception when the primary bookmark length is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetGetSecondaryIndexBookmarkThrowsExceptionWhenPrimaryKeyLengthIsTooLong()
        {
            int ignored1;
            int ignored2;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                null,
                0,
                out ignored1,
                new byte[2],
                3,
                out ignored2,
                GetSecondaryIndexBookmarkGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveKey gets 
        /// null data and a non-null length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveKey gets null data and a non-null length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveKeyThrowsExceptionWhenDataIsNull()
        {
            int actualSize;
            Api.JetRetrieveKey(this.sesid, this.tableid, null, 1, out actualSize, RetrieveKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetMakeKey gets a 
        /// data length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetMakeKey gets a data length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveKeyThrowsExceptionWhenDataLengthIsNegative()
        {
            var data = new byte[1];
            int actualSize;
            Api.JetRetrieveKey(this.sesid, this.tableid, data, -1, out actualSize, RetrieveKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetMakeKey gets a 
        /// data length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetMakeKey gets a data length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveKeyThrowsExceptionWhenDataLengthIsTooLong()
        {
            var data = new byte[1];
            int actualSize;
            Api.JetRetrieveKey(this.sesid, this.tableid, data, data.Length + 1, out actualSize, RetrieveKeyGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// null buffer and non-null length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a null buffer and non-null length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataIsNull()
        {
            int actualSize;
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, null, 1, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// data length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a data length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataSizeIsNegative()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, data, -1, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// data length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a data length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataSizeIsTooLong()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, data, data.Length + 1, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// data offset that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a data offset that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataOffsetIsNegative()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, data, data.Length, -1, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// data offset that is past the end of the data buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a data offset that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataOffsetIsTooLong()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, data, data.Length, data.Length, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// non-zero data offset with a null buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a non-zero data offset with a null buffer")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnThrowsExceptionWhenDataOffsetIsNonZeroAndBufferIsNull()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, null, 0, 1, out actualSize, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a negative itagSequence")]
        [ExpectedException(typeof(OverflowException))]
        public void JetRetrieveColumnThrowsExceptionWhenItagSequenceIsNegative()
        {
            int actualSize;
            var retinfo = new JET_RETINFO { itagSequence = -1 };
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, null, 0, out actualSize, RetrieveColumnGrbit.None, retinfo);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumn gets a 
        /// negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumn gets a negative ibLongValue")]
        [ExpectedException(typeof(OverflowException))]
        public void JetRetrieveColumnThrowsExceptionWhenIbLongValueIsNegative()
        {
            int actualSize;
            var retinfo = new JET_RETINFO { ibLongValue = -1 };
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnid, null, 0, out actualSize, RetrieveColumnGrbit.None, retinfo);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// null retrievecolumns array. 
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a null retrievecolumns array")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetRetrieveColumnsThrowsExceptionWhenRetrieveColumnsIsNull()
        {
            Api.JetRetrieveColumns(this.sesid, this.tableid, null, 0);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// negative number of columns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a negative number of columns")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnsThrowsExceptionWhenNumColumnsIsNegative()
        {
            Api.JetRetrieveColumns(this.sesid, this.tableid, new JET_RETRIEVECOLUMN[1], -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// numColumns count that is greater than the number of columns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a numColumns count that is greater than the number of columns")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnsThrowsExceptionWhenNumColumnsIsTooLong()
        {
            Api.JetRetrieveColumns(this.sesid, this.tableid, new JET_RETRIEVECOLUMN[1], 2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a negative itagSequence")]
        [ExpectedException(typeof(OverflowException))]
        public void JetRetrieveColumnsThrowsExceptionWhenItagSequenceIsNegative()
        {
            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN
                {
                    itagSequence = -1,
                },
            };
            Api.JetRetrieveColumns(this.sesid, this.tableid, retrievecolumns, retrievecolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a negative ibLongValue")]
        [ExpectedException(typeof(OverflowException))]
        public void JetRetrieveColumnsThrowsExceptionWhenIbLongValueIsNegative()
        {
            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN
                {
                    ibLongValue = -1,
                },
            };
            Api.JetRetrieveColumns(this.sesid, this.tableid, retrievecolumns, retrievecolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetRetrieveColumns gets a 
        /// cbData that is greater than the size of the pvData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetRetrieveColumns gets a cbData that is greater than the size of the pvData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetRetrieveColumnsThrowsExceptionWhenRetrieveColumnDataIsInvalid()
        {
            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN
                {
                    cbData = 100,
                    pvData = new byte[10],
                },
            };
            Api.JetRetrieveColumns(this.sesid, this.tableid, retrievecolumns, retrievecolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// null allocator callback.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEnumerateColumns gets a null allocator callback")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetEnumerateColumnsThrowsExceptionWhenAllocatorIsNull()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                0,
                null,
                out numColumnValues,
                out columnValues,
                null,
                IntPtr.Zero,
                0,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// negative maximum column size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEnumerateColumns gets a negative maximum column size")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEnumerateColumnsThrowsExceptionWhenMaxSizeIsNegative()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero;
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                0,
                null,
                out numColumnValues,
                out columnValues,
                allocator,
                IntPtr.Zero,
                -1,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// null columnids when numColumnids is non-zero.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description(" Check that an exception is thrown when JetEnumerateColumns gets a null columnids when numColumnids is non-zero")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEnumerateColumnsThrowsExceptionWhenColumnidsIsUnexpectedNull()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero;
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                1,
                null,
                out numColumnValues,
                out columnValues,
                allocator,
                IntPtr.Zero,
                0,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// negative numColumnids.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEnumerateColumns gets a negative numColumnids")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEnumerateColumnsThrowsExceptionWhenNumColumnidsIsNegative()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero;
            var columnids = new JET_ENUMCOLUMNID[2];
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                -1,
                columnids,
                out numColumnValues,
                out columnValues,
                allocator,
                IntPtr.Zero,
                0,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// numColumnids count which is greater than the size of columnids.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEnumerateColumns gets a numColumnids count which is greater than the size of columnids")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEnumerateColumnsThrowsExceptionWhenNumColumnidsIsTooLong()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero;
            var columnids = new JET_ENUMCOLUMNID[2];
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                columnids.Length + 1,
                columnids,
                out numColumnValues,
                out columnValues,
                allocator,
                IntPtr.Zero,
                0,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEnumerateColumns gets a 
        /// numColumnids count which is greater than the size of columnids.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEnumerateColumns gets a numColumnids count which is greater than the size of columnids")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEnumerateColumnsThrowsExceptionWhenNumColumnidRgtagIsInvalid()
        {
            int numColumnValues;
            JET_ENUMCOLUMN[] columnValues;
            JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero;
            var columnids = new[]
            {
                new JET_ENUMCOLUMNID { columnid = this.columnid, ctagSequence = 2, rgtagSequence = new int[1] },
            };
            Api.JetEnumerateColumns(
                this.sesid,
                this.tableid,
                columnids.Length,
                columnids,
                out numColumnValues,
                out columnValues,
                allocator,
                IntPtr.Zero,
                0,
                EnumerateColumnsGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when RetrieveColumns gets a 
        /// null columns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when RetrieveColumns gets null columns")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestRetrieveColumnsThrowsExceptionWhenColumnValuesIsNull()
        {
            Api.RetrieveColumns(this.sesid, this.tableid, null);
        }

        /// <summary>
        /// Check that an exception is thrown when RetrieveColumns gets a 
        /// zero-length array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when RetrieveColumns gets a zero-length array")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestRetrieveColumnsThrowsExceptionWhenColumnValuesIsZeroLength()
        {
            Api.RetrieveColumns(this.sesid, this.tableid, new ColumnValue[0]);
        }

        /// <summary>
        /// Check that an exception is thrown when RetrieveColumns gets too many columns
        /// to retrieve.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that an exception is thrown when RetrieveColumns gets too many columns to retrieve")]
        public void TestRetrieveColumnsThrowsExceptionWhenRetrievingTooManyColumns()
        {
            Api.RetrieveColumns(this.sesid, this.tableid, new ColumnValue[4096]);
        }

        #endregion

        #region DML

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// null buffer and non-null length (and SetSizeLV isn't specified).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a null buffer and non-null length (and SetSizeLV isn't specified)")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataIsNull()
        {
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, null, 1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a negative data length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataSizeIsNegative()
        {
            var data = new byte[1];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, -1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a negative data length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataSizeIsTooLong()
        {
            var data = new byte[1];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, data.Length + 1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// negative data offset.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a negative data offset")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataOffsetIsNegative()
        {
            var data = new byte[1];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, data.Length, -1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// data offset that is past the end of the buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a data offset that is past the end of the buffer")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataOffsetIsTooLong()
        {
            var data = new byte[1];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, 1, data.Length, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// non-zero data offset and a null buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a non-zero data offset and a null buffer")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataOffsetIsNonZeroAndBufferIsNull()
        {
            var data = new byte[1];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, null, 0, 1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// data offset that is past the end of the buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a data offset that is past the end of the buffer")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnThrowsExceptionWhenDataOffsetPlusSizeIsTooLong()
        {
            var data = new byte[4];
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, data.Length, 1, SetColumnGrbit.None, null);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a negative itagSequence")]
        [ExpectedException(typeof(OverflowException))]
        public void JetSetColumnThrowsExceptionWhenItagSequenceIsNegative()
        {
            var setinfo = new JET_SETINFO { itagSequence = -1 };
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, null, 0, SetColumnGrbit.None, setinfo);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumn gets a 
        /// negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumn gets a negative ibLongValue")]
        [ExpectedException(typeof(OverflowException))]
        public void JetSetColumnThrowsExceptionWhenIbLongValueIsNegative()
        {
            var setinfo = new JET_SETINFO { ibLongValue = -1 };
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, null, 0, SetColumnGrbit.None, setinfo);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// null setcolumns array. 
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a null setcolumns array")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetSetColumnsThrowsExceptionWhenSetColumnsIsNull()
        {
            JET_SETCOLUMN[] columns = null;
            Api.JetSetColumns(this.sesid, this.tableid, columns, 0);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// negative number of columns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a negative number of columns")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnsThrowsExceptionWhenNumColumnsIsNegative()
        {
            Api.JetSetColumns(this.sesid, this.tableid, new JET_SETCOLUMN[1], -1);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// numColumns count that is greater than the number of columns.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a numColumns count that is greater than the number of columns")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnsThrowsExceptionWhenNumColumnsIsTooLong()
        {
            Api.JetSetColumns(this.sesid, this.tableid, new JET_SETCOLUMN[1], 2);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// cbData that is greater than the size of the pvData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a cbData that is greater than the size of the pvData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnsThrowsExceptionWhenSetColumnDataIsInvalid()
        {
            var setcolumns = new[]
            {
                new JET_SETCOLUMN
                {
                    cbData = 100,
                    pvData = new byte[10],
                },
            };
            Api.JetSetColumns(this.sesid, this.tableid, setcolumns, setcolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a negative itagSequence")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnsThrowsExceptionWhenItagSequenceIsNegative()
        {
            var setcolumns = new[]
            {
                new JET_SETCOLUMN
                {
                    itagSequence = -1,
                },
            };
            Api.JetSetColumns(this.sesid, this.tableid, setcolumns, setcolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when JetSetColumns gets a 
        /// negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetSetColumns gets a negative ibLongValue")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetSetColumnsThrowsExceptionWhenIbLongValueIsNegative()
        {
            var setcolumns = new[]
            {
                new JET_SETCOLUMN
                {
                    ibLongValue = -1,
                },
            };
            Api.JetSetColumns(this.sesid, this.tableid, setcolumns, setcolumns.Length);
        }

        /// <summary>
        /// Check that an exception is thrown when SetColumns gets a 
        /// null value list.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when SetColumns gets a null value list")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void SetColumnsThrowsExceptionWhenColumnValuesIsNull()
        {
            Api.SetColumns(this.sesid, this.tableid, null);
        }

        /// <summary>
        /// Check that an exception is thrown when SetColumns gets a 
        /// zero-length array.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when SetColumns gets a zero-length array")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void SetColumnsThrowsExceptionWhenColumnValuesIsZeroLength()
        {
            Api.SetColumns(this.sesid, this.tableid, new ColumnValue[0]);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate gets a 
        /// null buffer and non-null length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetUpdate gets a null buffer and non-null length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdateThrowsExceptionWhenDataIsNull()
        {
            int actualSize;
            Api.JetUpdate(this.sesid, this.tableid, null, 1, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate gets a 
        /// data length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetUpdate gets a data length that is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdateThrowsExceptionWhenDataSizeIsNegative()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetUpdate(this.sesid, this.tableid, data, -1, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate gets a 
        /// data length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetUpdate gets a data length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdateThrowsExceptionWhenDataSizeIsTooLong()
        {
            int actualSize;
            var data = new byte[1];
            Api.JetUpdate(this.sesid, this.tableid, data, data.Length + 1, out actualSize);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate2 gets a 
        /// null buffer and non-zero length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetUpdate2 throws an exception when the buffer is null and the size is non-zero")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdate2ThrowsExceptionWhenDataIsNull()
        {
            if (!EsentVersion.SupportsServer2003Features)
            {
                throw new ArgumentOutOfRangeException();    
            }

            int actualSize;
            Server2003Api.JetUpdate2(this.sesid, this.tableid, null, 1, out actualSize, UpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate2 gets a 
        /// data length that is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetUpdate2 throws an exception when the buffer size is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdate2ThrowsExceptionWhenDataSizeIsNegative()
        {
            if (!EsentVersion.SupportsServer2003Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int actualSize;
            var data = new byte[1];
            Server2003Api.JetUpdate2(this.sesid, this.tableid, data, -1, out actualSize, UpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetUpdate2 gets a 
        /// data length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetUpdate2 throws an exception when the buffer size is longer than the buffer")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetUpdate2ThrowsExceptionWhenDataSizeIsTooLong()
        {
            if (!EsentVersion.SupportsServer2003Features)
            {
                throw new ArgumentOutOfRangeException();
            }

            int actualSize;
            var data = new byte[1];
            Server2003Api.JetUpdate2(this.sesid, this.tableid, data, data.Length + 1, out actualSize, UpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// null delta.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a null delta")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void JetEscrowUpdateThrowsExceptionWhenDeltaIsNull()
        {
            int actualSize;
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, null, 0, null, 0, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// negative delta length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a negative delta length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEscrowUpdateThrowsExceptionWhenDeltaSizeIsNegative()
        {
            int actualSize;
            var delta = new byte[4];
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, delta, -1, null, 0, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// delta length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a delta length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEscrowUpdateThrowsExceptionWhenDeltaSizeIsTooLong()
        {
            int actualSize;
            var delta = new byte[1];
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, delta, delta.Length + 1, null, 0, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// null previous value and non-zero length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a null previous value and non-zero length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEscrowUpdateThrowsExceptionWhenPreviousValueIsNull()
        {
            int actualSize;
            var delta = new byte[4];
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, delta, delta.Length, null, 4, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// negative previous value length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a negative previous value length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEscrowUpdateThrowsExceptionWhenPreviousValueSizeIsNegative()
        {
            int actualSize;
            var delta = new byte[4];
            var previous = new byte[4];
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, delta, delta.Length, previous, -1, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Check that an exception is thrown when JetEscrowUpdate gets a 
        /// previous value length that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when JetEscrowUpdate gets a previous value length that is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void JetEscrowUpdateThrowsExceptionWhenPreviousValueSizeIsTooLong()
        {
            int actualSize;
            var delta = new byte[4];
            var previous = new byte[4];
            Api.JetEscrowUpdate(this.sesid, this.tableid, this.columnid, delta, delta.Length, previous, previous.Length + 1, out actualSize, EscrowUpdateGrbit.None);
        }

        /// <summary>
        /// Create an Update with JET_prep.Cancel, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Create an Update with JET_prep.Cancel, expecting an exception.")]
        [ExpectedException(typeof(ArgumentException))]
        public void TestCreatingUpdatePrepCancelThrowsException()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Cancel);
        }

        #endregion

        #region Callbacks

        /// <summary>
        /// Verify that JetRegisterCallback throws an exception when the callback is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetRegisterCallback throws an exception when the callback is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyJetRegisterCallbackThrowsExceptionWhenCallbackIsNull()
        {
            JET_HANDLE ignored;
            Api.JetRegisterCallback(this.sesid, this.tableid, JET_cbtyp.BeforeReplace, null, IntPtr.Zero, out ignored);
        }

        #endregion

        /// <summary>
        /// Class used for testing JetCompact.
        /// </summary>
#pragma warning disable 612,618
        private class Converter : JET_CONVERT
#pragma warning restore 612,618
        {            
        }
    }
}
