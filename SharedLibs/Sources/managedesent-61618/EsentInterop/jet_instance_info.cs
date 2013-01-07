//-----------------------------------------------------------------------
// <copyright file="jet_instance_info.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_INSTANCE_INFO structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_INSTANCE_INFO
    {
        /// <summary>
        /// The JET_INSTANCE of the given instance.
        /// </summary>
        public IntPtr hInstanceId;

        /// <summary>
        /// The name of the database instance. This value can be NULL if the
        /// instance does not have a name.
        /// </summary>
        public IntPtr szInstanceName;

        /// <summary>
        /// The number of databases that are attached to the database instance.
        /// cDatabases also holds the size of the arrays of strings that are
        /// returned in szDatabaseFileName, szDatabaseDisplayName, and
        /// szDatabaseSLVFileName.
        /// </summary>
        public IntPtr cDatabases;

        /// <summary>
        /// An array of strings, each holding the file name of a database that
        /// is attached to the database instance. The array has cDatabases
        /// elements.
        /// </summary>
        public IntPtr* szDatabaseFileName;

        /// <summary>
        /// An array of strings, each holding the display name of a database.
        /// This string is always null. The array has cDatabases elements.
        /// </summary>
        public IntPtr* szDatabaseDisplayName;

        /// <summary>
        /// An array of strings, each holding the file name of the SLV file that
        /// is attached to the database instance. The array has cDatabases
        /// elements. SLV files are not supported, so this field should be ignored.
        /// </summary>
        [Obsolete("SLV files are not supported")]
        public IntPtr* szDatabaseSLVFileName;
    }

    /// <summary>
    /// Receives information about running database instances when used with the
    /// JetGetInstanceInfo and JetOSSnapshotFreeze functions.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_INSTANCE_INFO : IEquatable<JET_INSTANCE_INFO>
    {
        /// <summary>
        /// Collection of database file names.
        /// </summary>
        private ReadOnlyCollection<string> databases;

        /// <summary>
        /// Initializes a new instance of the <see cref="JET_INSTANCE_INFO"/> class.
        /// </summary>
        internal JET_INSTANCE_INFO()
        {            
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JET_INSTANCE_INFO"/> class.
        /// </summary>
        /// <param name="instance">
        /// The instance.
        /// </param>
        /// <param name="instanceName">
        /// The name of the instance.
        /// </param>
        /// <param name="databases">
        /// The databases in the instance.
        /// </param>
        internal JET_INSTANCE_INFO(JET_INSTANCE instance, string instanceName, string[] databases)
        {
            this.hInstanceId = instance;
            this.szInstanceName = instanceName;
            if (null == databases)
            {
                this.cDatabases = 0;
                this.databases = null;
            }
            else
            {
                this.cDatabases = databases.Length;
                this.databases = Array.AsReadOnly(databases);                
            }
        }

        /// <summary>
        /// Gets the JET_INSTANCE of the given instance.
        /// </summary>
        public JET_INSTANCE hInstanceId { get; private set; }

        /// <summary>
        /// Gets the name of the database instance. This value can be null if
        /// the instance does not have a name.
        /// </summary>
        public string szInstanceName { get; private set; }

        /// <summary>
        /// Gets the number of databases that are attached to the database instance.
        /// </summary>
        public int cDatabases { get; private set; }

        /// <summary>
        /// Gets a collection of strings, each holding the file name of a database
        /// that is attached to the database instance. The array has cDatabases
        /// elements.
        /// </summary>
        public IList<string> szDatabaseFileName
        {
            get
            {
                return this.databases;
            }
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_INSTANCE_INFO)obj);
        }

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_INSTANCE_INFO({0})", this.szInstanceName);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            int hash = this.hInstanceId.GetHashCode()
                   ^ (this.szInstanceName ?? String.Empty).GetHashCode()
                   ^ this.cDatabases << 20;

            for (int i = 0; i < this.cDatabases; ++i)
            {
                hash ^= this.szDatabaseFileName[i].GetHashCode();
            }

            return hash;
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_INSTANCE_INFO other)
        {
            if (null == other)
            {
                return false;
            }

            if (this.hInstanceId != other.hInstanceId
                || this.szInstanceName != other.szInstanceName
                || this.cDatabases != other.cDatabases)
            {
                return false;
            }

            for (int i = 0; i < this.cDatabases; ++i)
            {
                if (this.szDatabaseFileName[i] != other.szDatabaseFileName[i])
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Set the properties of the object from a native instance info where the
        /// strings in the NATIVE_INSTANCE_INFO are ASCII.
        /// </summary>
        /// <param name="native">The native instance info.</param>
        internal void SetFromNativeAscii(NATIVE_INSTANCE_INFO native)
        {
            this.hInstanceId = new JET_INSTANCE { Value = native.hInstanceId };
            this.szInstanceName = Marshal.PtrToStringAnsi(native.szInstanceName);
            this.cDatabases = checked((int)native.cDatabases);
            string[] files = new string[this.cDatabases];
            unsafe
            {
                for (int i = 0; i < this.cDatabases; ++i)
                {
                    files[i] = Marshal.PtrToStringAnsi(native.szDatabaseFileName[i]);
                }
            }

            this.databases = Array.AsReadOnly(files);
        }

        /// <summary>
        /// Set the properties of the object from a native instance info where the
        /// strings in the NATIVE_INSTANCE_INFO are Unicode.
        /// </summary>
        /// <param name="native">The native instance info.</param>
        internal void SetFromNativeUnicode(NATIVE_INSTANCE_INFO native)
        {
            this.hInstanceId = new JET_INSTANCE { Value = native.hInstanceId };
            this.szInstanceName = Marshal.PtrToStringUni(native.szInstanceName);
            this.cDatabases = checked((int)native.cDatabases);
            string[] files = new string[this.cDatabases];
            unsafe
            {
                for (int i = 0; i < this.cDatabases; ++i)
                {
                    files[i] = Marshal.PtrToStringUni(native.szDatabaseFileName[i]);
                }
            }

            this.databases = Array.AsReadOnly(files);
        }
    }
}