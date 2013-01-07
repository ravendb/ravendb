//-----------------------------------------------------------------------
// <copyright file="jet_tablecreate.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// The native version of the <see cref="JET_TABLECREATE"/> structure. This includes callbacks,
    /// space hints, and uses NATIVE_INDEXCREATE.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_TABLECREATE2
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Name of the table to create.
        /// </summary>
        public string szTableName;

        /// <summary>
        /// Name of the table from which to inherit base DDL.
        /// </summary>
        public string szTemplateTableName;

        /// <summary>
        /// Initial pages to allocate for table.
        /// </summary>
        public uint ulPages;

        /// <summary>
        /// Table density.
        /// </summary>
        public uint ulDensity;

        /// <summary>
        /// Array of column creation info.
        /// </summary>
        public NATIVE_COLUMNCREATE* rgcolumncreate;

        /// <summary>
        /// Number of columns to create.
        /// </summary>
        public uint cColumns;

        /// <summary>
        /// Array of indices to create, pointer to <see cref="NATIVE_INDEXCREATE"/>.
        /// </summary>
        public IntPtr rgindexcreate;

        /// <summary>
        /// Number of indices to create.
        /// </summary>
        public uint cIndexes;

        /// <summary>
        /// Callback function to use for the table.
        /// </summary>
        public string szCallback;

        /// <summary>
        /// Type of the callback function.
        /// </summary>
        public JET_cbtyp cbtyp;

        /// <summary>
        /// Table options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Returned tabledid.
        /// </summary>
        public IntPtr tableid;

        /// <summary>
        /// Count of objects created (columns+table+indexes+callbacks).
        /// </summary>
        public uint cCreated;
    }

    /// <summary>
    /// The native version of the <see cref="JET_TABLECREATE"/> structure. This includes callbacks,
    /// space hints, and uses NATIvE_INDEXCREATE2.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_TABLECREATE3
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Name of the table to create.
        /// </summary>
        public string szTableName;

        /// <summary>
        /// Name of the table from which to inherit base DDL.
        /// </summary>
        public string szTemplateTableName;

        /// <summary>
        /// Initial pages to allocate for table.
        /// </summary>
        public uint ulPages;

        /// <summary>
        /// Table density.
        /// </summary>
        public uint ulDensity;

        /// <summary>
        /// Array of column creation info.
        /// </summary>
        public NATIVE_COLUMNCREATE* rgcolumncreate;

        /// <summary>
        /// Number of columns to create.
        /// </summary>
        public uint cColumns;

        /// <summary>
        /// Array of indices to create, pointer to <see cref="NATIVE_INDEXCREATE2"/>.
        /// </summary>
        public IntPtr rgindexcreate;

        /// <summary>
        /// Number of indices to create.
        /// </summary>
        public uint cIndexes;

        /// <summary>
        /// Callback function to use for the table.
        /// </summary>
        public string szCallback;

        /// <summary>
        /// Type of the callback function.
        /// </summary>
        public JET_cbtyp cbtyp;

        /// <summary>
        /// Table options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Space allocation, maintenance, and usage hints for default sequential index.
        /// </summary>
        public NATIVE_SPACEHINTS* pSeqSpacehints;

        /// <summary>
        /// Space allocation, maintenance, and usage hints for Separated LV tree.
        /// </summary>
        public NATIVE_SPACEHINTS* pLVSpacehints;

        /// <summary>
        /// Heuristic size to separate a intrinsic LV from the primary record.
        /// </summary>
        public uint cbSeparateLV;

        /// <summary>
        /// Returned tabledid.
        /// </summary>
        public IntPtr tableid;

        /// <summary>
        /// Count of objects created (columns+table+indexes+callbacks).
        /// </summary>
        public uint cCreated;
    }

    /// <summary>
    /// Contains the information needed to create a table in an ESE database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public class JET_TABLECREATE : IContentEquatable<JET_TABLECREATE>, IDeepCloneable<JET_TABLECREATE>
    {
        /// <summary>
        /// Name of the table to create.
        /// </summary>
        private string tableName;

        /// <summary>
        /// Name of the table from which to inherit base DDL.
        /// </summary>
        private string templateTableName;

        /// <summary>
        /// Initial pages to allocate for table.
        /// </summary>
        private int initialPageAllocation;

        /// <summary>
        /// Table density.
        /// </summary>
        private int tableDensity;

        /// <summary>
        /// Array of column creation info.
        /// </summary>
        private JET_COLUMNCREATE[] columnCreates;

        /// <summary>
        /// Number of columns to create.
        /// </summary>
        private int columnCreateCount;

        /// <summary>
        /// Array of indices to create, pointer to <see cref="NATIVE_INDEXCREATE2"/>.
        /// </summary>
        private JET_INDEXCREATE[] indexCreates;

        /// <summary>
        /// Number of indices to create.
        /// </summary>
        private int indexCreateCount;

        /// <summary>
        /// Callback function to use for the table.
        /// </summary>
        private string callbackFunction;

        /// <summary>
        /// Type of the callback function.
        /// </summary>
        private JET_cbtyp callbackType;

        /// <summary>
        /// Table options.
        /// </summary>
        private CreateTableColumnIndexGrbit options;

        /// <summary>
        /// Space allocation, maintenance, and usage hints for default sequential index.
        /// </summary>
        private JET_SPACEHINTS seqSpacehints;

        /// <summary>
        /// Space allocation, maintenance, and usage hints for Separated LV tree.
        /// </summary>
        private JET_SPACEHINTS longValueSpacehints;

        /// <summary>
        /// Heuristic size to separate a intrinsic LV from the primary record.
        /// </summary>
        private int separateLvThresholdHint;

        /// <summary>
        /// Returned tabledid.
        /// </summary>
        [NonSerialized]
        private JET_TABLEID tableIdentifier;

        /// <summary>
        /// Count of objects created (columns+table+indexes+callbacks).
        /// </summary>
        private int objectsCreated;

        /// <summary>
        /// Gets or sets the name of the table to create.
        /// </summary>
        public string szTableName
        {
            [DebuggerStepThrough]
            get { return this.tableName; }
            set { this.tableName = value; }
        }

        /// <summary>
        /// Gets or sets the name of the table from which to inherit base DDL.
        /// </summary>
        public string szTemplateTableName
        {
            [DebuggerStepThrough]
            get { return this.templateTableName; }
            set { this.templateTableName = value; }
        }

        /// <summary>
        /// Gets or sets the initial pages to allocate for table.
        /// </summary>
        public int ulPages
        {
            [DebuggerStepThrough]
            get { return this.initialPageAllocation; }
            set { this.initialPageAllocation = value; }
        }

        /// <summary>
        /// Gets or sets the table density.
        /// </summary>
        public int ulDensity
        {
            [DebuggerStepThrough]
            get { return this.tableDensity; }
            set { this.tableDensity = value; }
        }

        /// <summary>
        /// Gets or sets an array of column creation info, of type <see cref="JET_COLUMNCREATE"/>.
        /// </summary>
        public JET_COLUMNCREATE[] rgcolumncreate
        {
            [DebuggerStepThrough]
            get { return this.columnCreates; }
            set { this.columnCreates = value; }
        }

        /// <summary>
        /// Gets or sets the number of columns to create.
        /// </summary>
        public int cColumns
        {
            [DebuggerStepThrough]
            get { return this.columnCreateCount; }
            set { this.columnCreateCount = value; }
        }

        /// <summary>
        /// Gets or sets an array of indices to create, of type <see cref="JET_INDEXCREATE"/>.
        /// </summary>
        public JET_INDEXCREATE[] rgindexcreate
        {
            [DebuggerStepThrough]
            get { return this.indexCreates; }
            set { this.indexCreates = value; }
        }

        /// <summary>
        /// Gets or sets the number of indices to create.
        /// </summary>
        public int cIndexes
        {
            [DebuggerStepThrough]
            get { return this.indexCreateCount; }
            set { this.indexCreateCount = value; }
        }

        /// <summary>
        /// Gets or sets a callback function to use for the table. This is in the form "module!functionName",
        /// and assumes unmanaged code. See <see cref="JetApi.JetRegisterCallback"/> for an alternative.
        /// </summary>
        public string szCallback
        {
            [DebuggerStepThrough]
            get { return this.callbackFunction; }
            set { this.callbackFunction = value; }
        }

        /// <summary>
        /// Gets or sets a type of the callback function.
        /// </summary>
        public JET_cbtyp cbtyp
        {
            [DebuggerStepThrough]
            get { return this.callbackType; }
            set { this.callbackType = value; }
        }

        /// <summary>
        /// Gets or sets the table options.
        /// </summary>
        public CreateTableColumnIndexGrbit grbit
        {
            [DebuggerStepThrough]
            get { return this.options; }
            set { this.options = value; }
        }

        /// <summary>
        /// Gets or sets space allocation, maintenance, and usage hints for default sequential index.
        /// </summary>
        public JET_SPACEHINTS pSeqSpacehints
        {
            [DebuggerStepThrough]
            get { return this.seqSpacehints; }
            set { this.seqSpacehints = value; }
        }

        /// <summary>
        /// Gets or sets space allocation, maintenance, and usage hints for Separated LV tree, of type <see cref="JET_SPACEHINTS"/>.
        /// </summary>
        public JET_SPACEHINTS pLVSpacehints
        {
            [DebuggerStepThrough]
            get { return this.longValueSpacehints; }
            set { this.longValueSpacehints = value; }
        }

        /// <summary>
        /// Gets or sets the heuristic size to separate a intrinsic LV from the primary record.
        /// </summary>
        public int cbSeparateLV
        {
            [DebuggerStepThrough]
            get { return this.separateLvThresholdHint; }
            set { this.separateLvThresholdHint = value; }
        }

        /// <summary>
        /// Gets or sets the returned tabledid.
        /// </summary>
        public JET_TABLEID tableid
        {
            [DebuggerStepThrough]
            get { return this.tableIdentifier; }
            set { this.tableIdentifier = value; }
        }

        /// <summary>
        /// Gets or sets the count of objects created (columns+table+indexes+callbacks).
        /// </summary>
        public int cCreated
        {
            [DebuggerStepThrough]
            get { return this.objectsCreated; }
            set { this.objectsCreated = value; }
        }

        #region IContentEquatable
        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_TABLECREATE other)
        {
            if (null == other)
            {
                return false;
            }

            this.CheckMembersAreValid();
            other.CheckMembersAreValid();

            return this.szTableName == other.szTableName
                && this.szTemplateTableName == other.szTemplateTableName
                && this.ulPages == other.ulPages
                && this.ulDensity == other.ulDensity
                && this.cColumns == other.cColumns
                && this.cIndexes == other.cIndexes
                && this.szCallback == other.szCallback
                && this.cbtyp == other.cbtyp
                && this.grbit == other.grbit
                && this.cbSeparateLV == other.cbSeparateLV
                && Util.ObjectContentEquals(this.pSeqSpacehints, other.pSeqSpacehints)
                && Util.ObjectContentEquals(this.pLVSpacehints, other.pLVSpacehints)
                && this.tableid == other.tableid
                && this.cCreated == other.cCreated
                && Util.ArrayObjectContentEquals(this.rgcolumncreate, other.rgcolumncreate, this.cColumns)
                && Util.ArrayObjectContentEquals(this.rgindexcreate, other.rgindexcreate, this.cIndexes);
        }
        #endregion

        #region IDeepCloneable
        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_TABLECREATE DeepClone()
        {
            JET_TABLECREATE result = (JET_TABLECREATE)this.MemberwiseClone();
            result.rgcolumncreate = Util.DeepCloneArray(this.rgcolumncreate);
            result.rgindexcreate = Util.DeepCloneArray(this.rgindexcreate);
            result.seqSpacehints = (null == this.seqSpacehints) ? null : this.seqSpacehints.DeepClone();
            result.pLVSpacehints = (null == this.pLVSpacehints) ? null : this.pLVSpacehints.DeepClone();

            return result;
        }

        #endregion

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_TABLECREATE({0}:{1} columns:{2} indices)",
                this.szTableName,
                this.cColumns,
                this.cIndexes);
        }

        /// <summary>
        /// Check this object to make sure its parameters are valid.
        /// </summary>
        internal void CheckMembersAreValid()
        {
            if (this.cColumns < 0)
            {
                throw new ArgumentOutOfRangeException("cColumns", this.cColumns, "cannot be negative");
            }

            if (this.rgcolumncreate != null && this.cColumns > this.rgcolumncreate.Length)
            {
                throw new ArgumentOutOfRangeException("cColumns", this.cColumns, "cannot be greater than rgcolumncreate.Length");
            }

            if (this.rgcolumncreate == null && this.cColumns != 0)
            {
                throw new ArgumentOutOfRangeException("cColumns", this.cColumns, "must be zero when rgcolumncreate is null");
            }

            if (this.cIndexes < 0)
            {
                throw new ArgumentOutOfRangeException("cIndexes", this.cIndexes, "cannot be negative");
            }

            if (this.rgindexcreate != null && this.cIndexes > this.rgindexcreate.Length)
            {
                throw new ArgumentOutOfRangeException("cIndexes", this.cIndexes, "cannot be greater than rgindexcreate.Length");
            }

            if (this.rgindexcreate == null && this.cIndexes != 0)
            {
                throw new ArgumentOutOfRangeException("cIndexes", this.cIndexes, "must be zero when rgindexcreate is null");
            }
        }

        /// <summary>
        /// Gets the native (interop) version of this object. The following members are
        /// NOT converted: <see cref="rgcolumncreate"/>, <see cref="rgindexcreate"/>,
        /// <see cref="pSeqSpacehints"/>, and <see cref="pLVSpacehints"/>.
        /// </summary>
        /// <returns>The native (interop) version of this object.</returns>
        internal NATIVE_TABLECREATE2 GetNativeTableCreate2()
        {
            this.CheckMembersAreValid();

            var native = new NATIVE_TABLECREATE2();
            native.cbStruct = checked((uint)Marshal.SizeOf(native));
            native.szTableName = this.szTableName;
            native.szTemplateTableName = this.szTemplateTableName;
            native.ulPages = checked((uint)this.ulPages);
            native.ulDensity = checked((uint)this.ulDensity);

            // native.rgcolumncreate is done at pinvoke time.
            native.cColumns = checked((uint)this.cColumns);

            // native.rgindexcreate is done at pinvoke time.
            native.cIndexes = checked((uint)this.cIndexes);
            native.szCallback = this.szCallback;
            native.cbtyp = this.cbtyp;
            native.grbit = checked((uint)this.grbit);
            native.tableid = this.tableid.Value;
            native.cCreated = checked((uint)this.cCreated);

            return native;
        }

        /// <summary>
        /// Gets the native (interop) version of this object. The following members are
        /// NOT converted: <see cref="rgcolumncreate"/>, <see cref="rgindexcreate"/>,
        /// <see cref="pSeqSpacehints"/>, and <see cref="pLVSpacehints"/>.
        /// </summary>
        /// <returns>The native (interop) version of this object.</returns>
        internal NATIVE_TABLECREATE3 GetNativeTableCreate3()
        {
            this.CheckMembersAreValid();

            var native = new NATIVE_TABLECREATE3();
            native.cbStruct = checked((uint)Marshal.SizeOf(native));
            native.szTableName = this.szTableName;
            native.szTemplateTableName = this.szTemplateTableName;
            native.ulPages = checked((uint)this.ulPages);
            native.ulDensity = checked((uint)this.ulDensity);

            // native.rgcolumncreate is done at pinvoke time.
            native.cColumns = checked((uint)this.cColumns);

            // native.rgindexcreate is done at pinvoke time.
            native.cIndexes = checked((uint)this.cIndexes);
            native.szCallback = this.szCallback;
            native.cbtyp = this.cbtyp;
            native.grbit = checked((uint)this.grbit);

            // native.pSeqSpacehints is done at pinvoke time.
            // native.pLVSpacehints is done at pinvoke time.
            native.cbSeparateLV = checked((uint)this.cbSeparateLV);
            native.tableid = this.tableid.Value;
            native.cCreated = checked((uint)this.cCreated);

            return native;
        }
    }
}
