//-----------------------------------------------------------------------
// <copyright file="jet_indexcreate.cs" company="Microsoft Corporation">
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
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// The native version of the JET_INDEXCREATE structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_INDEXCREATE
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Name of the index.
        /// </summary>
        public IntPtr szIndexName;

        /// <summary>
        /// Index key description.
        /// </summary>
        public IntPtr szKey;

        /// <summary>
        /// Size of index key description.
        /// </summary>
        public uint cbKey;

        /// <summary>
        /// Index options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Index density.
        /// </summary>
        public uint ulDensity;

        /// <summary>
        /// Pointer to unicode sort options.
        /// </summary>
        public NATIVE_UNICODEINDEX* pidxUnicode;

        /// <summary>
        /// Maximum size of column data to index. This can also be
        /// a pointer to a JET_TUPLELIMITS structure.
        /// </summary>
        public IntPtr cbVarSegMac;

        /// <summary>
        /// Pointer to array of conditional columns.
        /// </summary>
        public IntPtr rgconditionalcolumn;

        /// <summary>
        /// Count of conditional columns.
        /// </summary>
        public uint cConditionalColumn;

        /// <summary>
        /// Returned error from index creation.
        /// </summary>
        public int err;
    }

    /// <summary>
    /// The native version of the JET_INDEXCREATE structure. This version includes the cbKeyMost
    /// member, which is only valid on Windows Vista and above, but the name of the structure
    /// was not changed for Vista.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_INDEXCREATE1
    {
        /// <summary>
        /// Nested NATIVE_INDEXCREATE structure.
        /// </summary>
        public NATIVE_INDEXCREATE indexcreate;

        /// <summary>
        /// Maximum size of the key.
        /// </summary>
        public uint cbKeyMost;
    }

    /// <summary>
    /// The native version of the JET_INDEXCREATE2 structure. Introduced in Windows 7,
    /// this includes a <see cref="JET_SPACEHINTS"/> member.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_INDEXCREATE2
    {
        /// <summary>
        /// Nested NATIVE_INDEXCREATE1 structure.
        /// </summary>
        public NATIVE_INDEXCREATE1 indexcreate1;

        /// <summary>
        /// A <see cref="NATIVE_SPACEHINTS"/> pointer.
        /// </summary>
        public IntPtr pSpaceHints;
    }

    /// <summary>
    /// Contains the information needed to create an index over data in an ESE database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public sealed class JET_INDEXCREATE : IContentEquatable<JET_INDEXCREATE>, IDeepCloneable<JET_INDEXCREATE>
    {
        /// <summary>
        /// Name of the index.
        /// </summary>
        private string name;

        /// <summary>
        /// Index key.
        /// </summary>
        private string key;

        /// <summary>
        /// Length of the index key.
        /// </summary>
        private int keyLength;

        /// <summary>
        /// Index options.
        /// </summary>
        private CreateIndexGrbit options;

        /// <summary>
        /// Index density.
        /// </summary>
        private int density;

        /// <summary>
        /// Unicode comparison options.
        /// </summary>
        private JET_UNICODEINDEX unicodeOptions;

        /// <summary>
        /// Maximum length of a column to store in the index.
        /// </summary>
        private int maxSegmentLength;

        /// <summary>
        /// Conditional columns.
        /// </summary>
        private JET_CONDITIONALCOLUMN[] conditionalColumns;

        /// <summary>
        /// Number of conditional columns.
        /// </summary>
        private int numConditionalColumns;

        /// <summary>
        /// Error code from index creation.
        /// </summary>
        private JET_err errorCode;

        /// <summary>
        /// Maximum length of index keys.
        /// </summary>
        private int maximumKeyLength;

        /// <summary>
        /// Space allocation, maintenance, and usage hints.
        /// </summary>
        private JET_SPACEHINTS spaceHints;

        /// <summary>
        /// Gets or sets the error code from creating this index.
        /// </summary>
        public JET_err err
        {
            [DebuggerStepThrough]
            get { return this.errorCode; }
            set { this.errorCode = value; }
        }

        /// <summary>
        /// Gets or sets the name of the index to create. 
        /// </summary>
        public string szIndexName
        {
            [DebuggerStepThrough]
            get { return this.name; }
            set { this.name = value; }
        }

        /// <summary>
        /// Gets or sets the description of the index key. This is a double 
        /// null-terminated string of null-delimited tokens. Each token is
        /// of the form [direction-specifier][column-name], where
        /// direction-specification is either "+" or "-". for example, a
        /// szKey of "+abc\0-def\0+ghi\0" will index over the three columns
        /// "abc" (in ascending order), "def" (in descending order), and "ghi"
        /// (in ascending order).
        /// </summary>
        public string szKey
        {
            [DebuggerStepThrough]
            get { return this.key; }
            set { this.key = value; }
        }

        /// <summary>
        /// Gets or sets the length, in characters, of szKey including the two terminating nulls.
        /// </summary>
        public int cbKey
        {
            [DebuggerStepThrough]
            get { return this.keyLength; }
            set { this.keyLength = value; }
        }

        /// <summary>
        /// Gets or sets index creation options.
        /// </summary>
        public CreateIndexGrbit grbit
        {
            [DebuggerStepThrough]
            get { return this.options; }
            set { this.options = value; }
        }

        /// <summary>
        /// Gets or sets the density of the index.
        /// </summary>
        public int ulDensity
        {
            [DebuggerStepThrough]
            get { return this.density; }
            set { this.density = value; }
        }

        /// <summary>
        /// Gets or sets the optional unicode comparison options.
        /// </summary>
        public JET_UNICODEINDEX pidxUnicode
        {
            [DebuggerStepThrough]
            get { return this.unicodeOptions; }
            set { this.unicodeOptions = value; }
        }

        /// <summary>
        /// Gets or sets the maximum length, in bytes, of each column to store in the index.
        /// </summary>
        public int cbVarSegMac
        {
            [DebuggerStepThrough]
            get { return this.maxSegmentLength; }
            set { this.maxSegmentLength = value; }
        }

        /// <summary>
        /// Gets or sets the optional conditional columns.
        /// </summary>
        public JET_CONDITIONALCOLUMN[] rgconditionalcolumn
        {
            [DebuggerStepThrough]
            get { return this.conditionalColumns; }
            set { this.conditionalColumns = value; }
        }

        /// <summary>
        /// Gets or sets the number of conditional columns.
        /// </summary>
        public int cConditionalColumn
        {
            [DebuggerStepThrough]
            get { return this.numConditionalColumns; }
            set { this.numConditionalColumns = value; }
        }

        /// <summary>
        /// Gets or sets the maximum allowable size, in bytes, for keys in the index.
        /// The minimum supported maximum key size is JET_cbKeyMostMin (255) which
        /// is the legacy maximum key size. The maximum key size is dependent on
        /// the database page size <see cref="JET_param.DatabasePageSize"/>. The
        /// maximum key size can be retrieved with <see cref="SystemParameters.KeyMost"/>.
        /// <para>
        /// This parameter is ignored on Windows XP and Windows Server 2003.
        /// </para>
        /// <para>
        /// Unlike the unmanaged API, <see cref="VistaGrbits.IndexKeyMost"/>
        /// (JET_bitIndexKeyMost) is not needed, it will be added automatically.
        /// </para>
        /// </summary>
        public int cbKeyMost
        {
            [DebuggerStepThrough]
            get { return this.maximumKeyLength; }
            set { this.maximumKeyLength = value; }
        }

        /// <summary>
        /// Gets or sets space allocation, maintenance, and usage hints.
        /// </summary>
        public JET_SPACEHINTS pSpaceHints
        {
            [DebuggerStepThrough]
            get { return this.spaceHints; }
            set { this.spaceHints = value; }
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_INDEXCREATE DeepClone()
        {
            JET_INDEXCREATE result = (JET_INDEXCREATE)this.MemberwiseClone();
            result.pidxUnicode = (null == this.pidxUnicode) ? null : this.pidxUnicode.DeepClone();
            this.conditionalColumns = Util.DeepCloneArray(this.conditionalColumns);
            return result;
        }

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_INDEXCREATE({0}:{1})", this.szIndexName, this.szKey);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_INDEXCREATE other)
        {
            if (null == other)
            {
                return false;
            }

            this.CheckMembersAreValid();
            other.CheckMembersAreValid();
            return this.err == other.err
                            && this.szIndexName == other.szIndexName
                            && this.szKey == other.szKey
                            && this.cbKey == other.cbKey
                            && this.grbit == other.grbit
                            && this.ulDensity == other.ulDensity
                            && this.cbVarSegMac == other.cbVarSegMac
                            && this.cbKeyMost == other.cbKeyMost
                            && this.IsUnicodeIndexEqual(other)
                            && this.AreConditionalColumnsEqual(other);
        }

        /// <summary>
        /// Check this object to make sure its parameters are valid.
        /// </summary>
        internal void CheckMembersAreValid()
        {
            if (null == this.szIndexName)
            {
                throw new ArgumentNullException("szIndexName");
            }

            if (null == this.szKey)
            {
                throw new ArgumentNullException("szKey");
            }

            if (this.cbKey > checked(this.szKey.Length + 1))
            {
                throw new ArgumentOutOfRangeException("cbKey", this.cbKey, "cannot be greater than the length of szKey");
            }

            if (this.cbKey < 0)
            {
                throw new ArgumentOutOfRangeException("cbKey", this.cbKey, "cannot be negative");
            }

            if (this.ulDensity < 0)
            {
                throw new ArgumentOutOfRangeException("ulDensity", this.ulDensity, "cannot be negative");
            }

            if (this.cbKeyMost < 0)
            {
                throw new ArgumentOutOfRangeException("cbKeyMost", this.cbKeyMost, "cannot be negative");
            }

            if (this.cbVarSegMac < 0)
            {
                throw new ArgumentOutOfRangeException("cbVarSegMac", this.cbVarSegMac, "cannot be negative");
            }

            if ((this.cConditionalColumn > 0 && null == this.rgconditionalcolumn)
                || (this.cConditionalColumn > 0 && this.cConditionalColumn > this.rgconditionalcolumn.Length))
            {
                throw new ArgumentOutOfRangeException("cConditionalColumn", this.cConditionalColumn, "cannot be greater than the length of rgconditionalcolumn");
            }

            if (this.cConditionalColumn < 0)
            {
                throw new ArgumentOutOfRangeException(
                    "cConditionalColumn", this.cConditionalColumn, "cannot be negative");
            }
        }

        /// <summary>
        /// Gets the native (interop) version of this object, except for
        /// <see cref="szIndexName"/> and <see cref="szKey"/>.
        /// </summary>
        /// <returns>The native (interop) version of this object.</returns>
        internal NATIVE_INDEXCREATE GetNativeIndexcreate()
        {
            this.CheckMembersAreValid();

            var native = new NATIVE_INDEXCREATE();
            native.cbStruct = checked((uint)Marshal.SizeOf(native));

            // szIndexName and szKey are converted at pinvoke time.
            //
            // native.szIndexName = this.szIndexName;
            // native.szKey = this.szKey;
            native.cbKey = checked((uint)this.cbKey);
            native.grbit = unchecked((uint)this.grbit);
            native.ulDensity = checked((uint)this.ulDensity);

            native.cbVarSegMac = new IntPtr(this.cbVarSegMac);

            native.cConditionalColumn = checked((uint)this.cConditionalColumn);
            return native;
        }

        /// <summary>
        /// Gets the native (interop) version of this object, except for
        /// <see cref="szIndexName"/> and <see cref="szKey"/>.
        /// </summary>
        /// <returns>The native (interop) version of this object.</returns>
        internal NATIVE_INDEXCREATE1 GetNativeIndexcreate1()
        {
            var native = new NATIVE_INDEXCREATE1();
            native.indexcreate = this.GetNativeIndexcreate();
            native.indexcreate.cbStruct = checked((uint)Marshal.SizeOf(native));
            if (0 != this.cbKeyMost)
            {
                native.cbKeyMost = checked((uint)this.cbKeyMost);
                native.indexcreate.grbit |= unchecked((uint)VistaGrbits.IndexKeyMost);
            }

            return native;
        }

        /// <summary>
        /// Gets the native (interop) version of this object. The following members
        /// are not converted:
        /// <see cref="szIndexName"/>, <see cref="szKey"/>, <see cref="pSpaceHints"/>.
        /// </summary>
        /// <returns>The native (interop) version of this object.</returns>
        internal NATIVE_INDEXCREATE2 GetNativeIndexcreate2()
        {
            var native = new NATIVE_INDEXCREATE2();
            native.indexcreate1 = this.GetNativeIndexcreate1();
            native.indexcreate1.indexcreate.cbStruct = checked((uint)Marshal.SizeOf(native));

            // pSpaceHints conversion is done at pinvoke time.
            return native;
        }

        /// <summary>
        /// Returns a value indicating whether the pidxUnicode member of this
        /// instance is equal to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the pidxUnicode members of two instances are equal.</returns>
        private bool IsUnicodeIndexEqual(JET_INDEXCREATE other)
        {
            return (null == this.pidxUnicode)
                       ? (null == other.pidxUnicode)
                       : this.pidxUnicode.ContentEquals(other.pidxUnicode);
        }

        /// <summary>
        /// Returns a value indicating whether the conditional column members of this
        /// instance is equal to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the conditional column members of two instances are equal.</returns>
        private bool AreConditionalColumnsEqual(JET_INDEXCREATE other)
        {
            if (this.cConditionalColumn != other.cConditionalColumn)
            {
                return false;
            }

            for (int i = 0; i < this.cConditionalColumn; ++i)
            {
                if (!this.rgconditionalcolumn[i].ContentEquals(other.rgconditionalcolumn[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }
}

