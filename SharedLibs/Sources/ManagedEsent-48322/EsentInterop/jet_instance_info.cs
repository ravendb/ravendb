//-----------------------------------------------------------------------
// <copyright file="jet_instance_info.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
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
    public class JET_INSTANCE_INFO
    {
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
        /// Gets an array of strings, each holding the file name of a database
        /// that is attached to the database instance. The array has cDatabases
        /// elements.
        /// </summary>
        public string[] szDatabaseFileName { get; private set; }

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
            this.szDatabaseFileName = new string[this.cDatabases];
            unsafe
            {
                for (int i = 0; i < this.cDatabases; ++i)
                {
                    this.szDatabaseFileName[i] = Marshal.PtrToStringAnsi(native.szDatabaseFileName[i]);
                }
            }
        }

        /// <summary>
        /// Set the properties of the object from a native instance info where the
        /// strings i nthe NATIVE_INSTANCE_INFO are Unicode.
        /// </summary>
        /// <param name="native">The native instance info.</param>
        internal void SetFromNativeUnicode(NATIVE_INSTANCE_INFO native)
        {
            this.hInstanceId = new JET_INSTANCE { Value = native.hInstanceId };
            this.szInstanceName = Marshal.PtrToStringUni(native.szInstanceName);
            this.cDatabases = checked((int)native.cDatabases);
            this.szDatabaseFileName = new string[this.cDatabases];
            unsafe
            {
                for (int i = 0; i < this.cDatabases; ++i)
                {
                    this.szDatabaseFileName[i] = Marshal.PtrToStringUni(native.szDatabaseFileName[i]);
                }
            }
        }
    }
}
