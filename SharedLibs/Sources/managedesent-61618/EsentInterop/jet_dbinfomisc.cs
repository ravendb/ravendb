//-----------------------------------------------------------------------
// <copyright file="jet_dbinfomisc.cs" company="Microsoft Corporation">
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

    /// <summary>
    /// The native version of the JET_DBINFOMISC structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_DBINFOMISC
    {
        /// <summary>
        /// Version of Esent that created the database.
        /// </summary>
        public uint ulVersion;

        /// <summary>
        /// Incremental version of Esent that created the database.
        /// </summary>
        public uint ulUpdate;

        /// <summary>
        /// Database signature.
        /// </summary>
        public NATIVE_SIGNATURE signDb;

        /// <summary>
        /// Consistent/inconsistent state.
        /// </summary>
        public uint dbstate;

        /// <summary>
        /// Null if in inconsistent state.
        /// </summary>
        public JET_LGPOS lgposConsistent;

        /// <summary>
        /// Null if in inconsistent state.
        /// </summary>
        public JET_LOGTIME logtimeConsistent;

        /// <summary>
        /// Last attach time.
        /// </summary>
        public JET_LOGTIME logtimeAttach;

        /// <summary>
        /// Lgpos at last attach.
        /// </summary>
        public JET_LGPOS lgposAttach;

        /// <summary>
        /// Last detach time.
        /// </summary>
        public JET_LOGTIME logtimeDetach;

        /// <summary>
        /// Lgpos at last detach.
        /// </summary>
        public JET_LGPOS lgposDetach;

        /// <summary>
        /// Logfile signature.
        /// </summary>
        public NATIVE_SIGNATURE signLog;

        /// <summary>
        /// Last successful full backup.
        /// </summary>
        public JET_BKINFO bkinfoFullPrev;

        /// <summary>
        /// Last successful incremental backup. Reset when 
        /// <see cref="bkinfoFullPrev"/> is set.
        /// </summary>
        public JET_BKINFO bkinfoIncPrev;

        /// <summary>
        /// Current backup.
        /// </summary>
        public JET_BKINFO bkinfoFullCur;

        /// <summary>
        /// Internal use only.
        /// </summary>
        public uint fShadowingDisabled;

        /// <summary>
        /// Internal use only.
        /// </summary>
        public uint fUpgradeDb;

        /// <summary>
        /// OS major version.
        /// </summary>
        public uint dwMajorVersion;

        /// <summary>
        /// OS minor version.
        /// </summary>
        public uint dwMinorVersion;

        /// <summary>
        /// OS build number.
        /// </summary>
        public uint dwBuildNumber;

        /// <summary>
        /// OS Service Pack number.
        /// </summary>
        public uint lSPNumber;

        /// <summary>
        /// Database page size (0 = 4Kb page).
        /// </summary>
        public uint cbPageSize;
    }

    /// <summary>
    /// Native version of the JET_DBINFOMISC structure.
    /// Adds support for fields that we added in Windows 7.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_DBINFOMISC4
    {
        /// <summary>
        /// The core dbinfo structure.
        /// </summary>
        public NATIVE_DBINFOMISC dbinfo;

        // Fields added in JET_DBINFOMISC2

        /// <summary>
        /// The minimum log generation required for replaying the logs.
        /// Typically the checkpoint generation.
        /// </summary>
        public uint genMinRequired;

        /// <summary>
        /// The maximum log generation required for replaying the logs.
        /// </summary>
        public uint genMaxRequired;

        /// <summary>
        /// Creation time of the <see cref="genMaxRequired"/> logfile.
        /// </summary>
        public JET_LOGTIME logtimeGenMaxCreate;

        /// <summary>
        /// Number of times repair has been called on this database.
        /// </summary>
        public uint ulRepairCount;

        /// <summary>
        /// The last time that repair was run against this database.
        /// </summary>
        public JET_LOGTIME logtimeRepair;

        /// <summary>
        /// Number of times this database was repaired before the last defrag.
        /// </summary>
        public uint ulRepairCountOld;

        /// <summary>
        /// Number of times a one bit error was successfully fixed.
        /// </summary>
        public uint ulECCFixSuccess;

        /// <summary>
        /// The last time a one bit error was successfully fixed.
        /// </summary>
        public JET_LOGTIME logtimeECCFixSuccess;

        /// <summary>
        /// The number of times a one bit error was successfully fixed before the last repair.
        /// </summary>
        public uint ulECCFixSuccessOld;

        /// <summary>
        /// Number of times an uncorrectable one bit error was encountered.
        /// </summary>
        public uint ulECCFixFail;

        /// <summary>
        /// The last time an uncorrectable one bit error was encountered.
        /// </summary>
        public JET_LOGTIME logtimeECCFixFail;

        /// <summary>
        /// The number of times an uncorrectable one bit error was encountered.
        /// </summary>
        public uint ulECCFixFailOld;

        /// <summary>
        /// Number of times a non-correctable checksum error was found.
        /// </summary>
        public uint ulBadChecksum;

        /// <summary>
        /// The last time a non-correctable checksum error was found.
        /// </summary>
        public JET_LOGTIME logtimeBadChecksum;

        /// <summary>
        /// The number of times a non-correctable checksum error was found before the last repair.
        /// </summary>
        public uint ulBadChecksumOld;

        // Fields added in JET_DBINFOMISC3

        /// <summary>
        /// The maximum log generation committed to the database. Typically the current log generation.
        /// </summary>
        public uint genCommitted;

        // Fields added in JET_DBINFOMISC4

        /// <summary>
        /// Last successful copy backup.
        /// </summary>
        public JET_BKINFO bkinfoCopyPrev;

        /// <summary>
        /// Last successful differential backup. Reset when 
        /// bkinfoFullPrev is set.
        /// </summary>
        public JET_BKINFO bkinfoDiffPrev;
    }

    /// <summary>
    /// Holds miscellaneous information about a database. This is
    /// the information that is contained in the database header.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1309:FieldNamesMustNotBeginWithUnderscore",
        Justification = "Need to avoid clash between members and properties.")]
    [Serializable]
    public sealed partial class JET_DBINFOMISC : IEquatable<JET_DBINFOMISC>
    {
        /// <summary>
        /// Version of Esent that created the database.
        /// </summary>
        private int _ulVersion;

        /// <summary>
        /// Incremental version of Esent that created the database.
        /// </summary>
        private int _ulUpdate;

        /// <summary>
        /// Database signature.
        /// </summary>
        private JET_SIGNATURE _signDb;

        /// <summary>
        /// Consistent/inconsistent state.
        /// </summary>
        private JET_dbstate _dbstate;

        /// <summary>
        /// Null if in inconsistent state.
        /// </summary>
        private JET_LGPOS _lgposConsistent;

        /// <summary>
        /// Null if in inconsistent state.
        /// </summary>
        private JET_LOGTIME _logtimeConsistent;

        /// <summary>
        /// Last attach time.
        /// </summary>
        private JET_LOGTIME _logtimeAttach;

        /// <summary>
        /// Lgpos at last attach.
        /// </summary>
        private JET_LGPOS _lgposAttach;

        /// <summary>
        /// Last detach time.
        /// </summary>
        private JET_LOGTIME _logtimeDetach;

        /// <summary>
        /// Lgpos at last detach.
        /// </summary>
        private JET_LGPOS _lgposDetach;

        /// <summary>
        /// Logfile signature.
        /// </summary>
        private JET_SIGNATURE _signLog;

        /// <summary>
        /// Last successful full backup.
        /// </summary>
        private JET_BKINFO _bkinfoFullPrev;

        /// <summary>
        /// Last successful incremental backup. Reset when 
        /// <see cref="_bkinfoFullPrev"/> is set.
        /// </summary>
        private JET_BKINFO _bkinfoIncPrev;

        /// <summary>
        /// Current backup.
        /// </summary>
        private JET_BKINFO _bkinfoFullCur;

        /// <summary>
        /// Internal use only.
        /// </summary>
        private bool _fShadowingDisabled;

        /// <summary>
        /// Internal use only.
        /// </summary>
        private bool _fUpgradeDb;

        /// <summary>
        /// OS major version.
        /// </summary>
        private int _dwMajorVersion;

        /// <summary>
        /// OS minor version.
        /// </summary>
        private int _dwMinorVersion;

        /// <summary>
        /// OS build number.
        /// </summary>
        private int _dwBuildNumber;

        /// <summary>
        /// OS Service Pack number.
        /// </summary>
        private int _lSPNumber;

        /// <summary>
        /// Database page size (0 = 4Kb page).
        /// </summary>
        private int _cbPageSize;

        /// <summary>
        /// The minimum log generation required for replaying the logs.
        /// Typically the checkpoint generation.
        /// </summary>
        private int _genMinRequired;

        /// <summary>
        /// The maximum log generation required for replaying the logs.
        /// </summary>
        private int _genMaxRequired;

        /// <summary>
        /// Creation time of the <see cref="_genMaxRequired"/> logfile.
        /// </summary>
        private JET_LOGTIME _logtimeGenMaxCreate;

        /// <summary>
        /// Number of times repair has been called on this database.
        /// </summary>
        private int _ulRepairCount;

        /// <summary>
        /// The last time that repair was run against this database.
        /// </summary>
        private JET_LOGTIME _logtimeRepair;

        /// <summary>
        /// Number of times this database was repaired before the last defrag.
        /// </summary>
        private int _ulRepairCountOld;

        /// <summary>
        /// Number of times a one bit error was successfully fixed.
        /// </summary>
        private int _ulECCFixSuccess;

        /// <summary>
        /// The last time a one bit error was successfully fixed.
        /// </summary>
        private JET_LOGTIME _logtimeECCFixSuccess;

        /// <summary>
        /// The number of times a one bit error was successfully fixed before the last repair.
        /// </summary>
        private int _ulECCFixSuccessOld;

        /// <summary>
        /// Number of times an uncorrectable one bit error was encountered.
        /// </summary>
        private int _ulECCFixFail;

        /// <summary>
        /// The last time an uncorrectable one bit error was encountered.
        /// </summary>
        private JET_LOGTIME _logtimeECCFixFail;

        /// <summary>
        /// The number of times an uncorrectable one bit error was encountered.
        /// </summary>
        private int _ulECCFixFailOld;

        /// <summary>
        /// Number of times a non-correctable checksum error was found.
        /// </summary>
        private int _ulBadChecksum;

        /// <summary>
        /// The last time a non-correctable checksum error was found.
        /// </summary>
        private JET_LOGTIME _logtimeBadChecksum;

        /// <summary>
        /// The number of times a non-correctable checksum error was found before the last repair.
        /// </summary>
        private int _ulBadChecksumOld;

        /// <summary>
        /// The maximum log generation committed to the database. Typically the current log generation.
        /// </summary>
        private int _genCommitted;

        /// <summary>
        /// Last successful copy backup.
        /// </summary>
        private JET_BKINFO _bkinfoCopyPrev;

        /// <summary>
        /// Last successful differential backup. Reset when 
        /// <see cref="_bkinfoFullPrev"/> is set.
        /// </summary>
        private JET_BKINFO _bkinfoDiffPrev;

        /// <summary>
        /// Gets the version of Esent that created the database.
        /// </summary>
        public int ulVersion
        {
            [DebuggerStepThrough]
            get { return this._ulVersion; }
            internal set { this._ulVersion = value; }
        }

        /// <summary>
        /// Gets the incremental version of Esent that created the database.
        /// </summary>
        public int ulUpdate
        {
            [DebuggerStepThrough]
            get { return this._ulUpdate; }
            internal set { this._ulUpdate = value; }
        }

        /// <summary>
        /// Gets the database signature.
        /// </summary>
        public JET_SIGNATURE signDb
        {
            [DebuggerStepThrough]
            get { return this._signDb; }
            internal set { this._signDb = value; }
        }

        /// <summary>
        /// Gets the consistent/inconsistent state of the database.
        /// </summary>
        public JET_dbstate dbstate
        {
            [DebuggerStepThrough]
            get { return this._dbstate; }
            internal set { this._dbstate = value; }
        }

        /// <summary>
        /// Gets the lgpos when the database was made consistent.
        /// This value is null if the database is inconsistent.
        /// </summary>
        public JET_LGPOS lgposConsistent
        {
            [DebuggerStepThrough]
            get { return this._lgposConsistent; }
            internal set { this._lgposConsistent = value; }
        }

        /// <summary>
        /// Gets the time when the database was made consistent.
        /// This value is null if the database is inconsistent.
        /// </summary>
        public JET_LOGTIME logtimeConsistent
        {
            [DebuggerStepThrough]
            get { return this._logtimeConsistent; }
            internal set { this._logtimeConsistent = value; }
        }

        /// <summary>
        /// Gets the time when the database was attached.
        /// </summary>
        public JET_LOGTIME logtimeAttach
        {
            [DebuggerStepThrough]
            get { return this._logtimeAttach; }
            internal set { this._logtimeAttach = value; }
        }

        /// <summary>
        /// Gets the lgpos of the last attach.
        /// </summary>
        public JET_LGPOS lgposAttach
        {
            [DebuggerStepThrough]
            get { return this._lgposAttach; }
            internal set { this._lgposAttach = value; }
        }

        /// <summary>
        /// Gets the time of the last detach.
        /// </summary>
        public JET_LOGTIME logtimeDetach
        {
            [DebuggerStepThrough]
            get { return this._logtimeDetach; }
            internal set { this._logtimeDetach = value; }
        }

        /// <summary>
        /// Gets the lgpos of the last detach.
        /// </summary>
        public JET_LGPOS lgposDetach
        {
            [DebuggerStepThrough]
            get { return this._lgposDetach; }
            internal set { this._lgposDetach = value; }
        }

        /// <summary>
        /// Gets the logfile signature of logs used to modify the database.
        /// </summary>
        public JET_SIGNATURE signLog
        {
            [DebuggerStepThrough]
            get { return this._signLog; }
            internal set { this._signLog = value; }
        }

        /// <summary>
        /// Gets information about the last successful full backup.
        /// </summary>
        public JET_BKINFO bkinfoFullPrev
        {
            [DebuggerStepThrough]
            get { return this._bkinfoFullPrev; }
            internal set { this._bkinfoFullPrev = value; }
        }

        /// <summary>
        /// Gets information about the last successful incremental backup.
        /// This value is reset when <see cref="bkinfoFullPrev"/> is set.
        /// </summary>
        public JET_BKINFO bkinfoIncPrev
        {
            [DebuggerStepThrough]
            get { return this._bkinfoIncPrev; }
            internal set { this._bkinfoIncPrev = value; }
        }

        /// <summary>
        /// Gets information about the current backup.
        /// </summary>
        public JET_BKINFO bkinfoFullCur
        {
            [DebuggerStepThrough]
            get { return this._bkinfoFullCur; }
            internal set { this._bkinfoFullCur = value; }
        }

        /// <summary>
        /// Gets a value indicating whether catalog shadowing is enabled.
        /// This value is for internal use only.
        /// </summary>
        public bool fShadowingDisabled
        {
            [DebuggerStepThrough]
            get { return this._fShadowingDisabled; }
            internal set { this._fShadowingDisabled = value; }
        }

        /// <summary>
        /// Gets a value indicating whether the database is being upgraded.
        /// This value is for internal use only.
        /// </summary>
        public bool fUpgradeDb
        {
            [DebuggerStepThrough]
            get { return this._fUpgradeDb; }
            internal set { this._fUpgradeDb = value; }
        }

        /// <summary>
        /// Gets the OS major version from the last attach.
        /// </summary>
        public int dwMajorVersion
        {
            [DebuggerStepThrough]
            get { return this._dwMajorVersion; }
            internal set { this._dwMajorVersion = value; }
        }

        /// <summary>
        /// Gets the OS minor version from the last attach.
        /// </summary>
        public int dwMinorVersion
        {
            [DebuggerStepThrough]
            get { return this._dwMinorVersion; }
            internal set { this._dwMinorVersion = value; }
        }

        /// <summary>
        /// Gets the OS build number from the last attach.
        /// </summary>
        public int dwBuildNumber
        {
            [DebuggerStepThrough]
            get { return this._dwBuildNumber; }
            internal set { this._dwBuildNumber = value; }
        }

        /// <summary>
        /// Gets the OS Service Pack number from the last attach.
        /// </summary>
        public int lSPNumber
        {
            [DebuggerStepThrough]
            get { return this._lSPNumber; }
            internal set { this._lSPNumber = value; }
        }

        /// <summary>
        /// Gets the database page size. A value of 0 means 4Kb pages.
        /// </summary>
        public int cbPageSize
        {
            [DebuggerStepThrough]
            get { return this._cbPageSize; }
            internal set { this._cbPageSize = value; }
        }

        /// <summary>
        /// Gets the minimum log generation required for replaying the logs.
        /// Typically the checkpoint generation.
        /// </summary>
        public int genMinRequired
        {
            [DebuggerStepThrough]
            get { return this._genMinRequired; }
            internal set { this._genMinRequired = value; }
        }

        /// <summary>
        /// Gets the maximum log generation required for replaying the logs.
        /// </summary>
        public int genMaxRequired
        {
            [DebuggerStepThrough]
            get { return this._genMaxRequired; }
            internal set { this._genMaxRequired = value; }
        }

        /// <summary>
        /// Gets the creation time of the <see cref="genMaxRequired"/> logfile.
        /// </summary>
        public JET_LOGTIME logtimeGenMaxCreate
        {
            [DebuggerStepThrough]
            get { return this._logtimeGenMaxCreate; }
            internal set { this._logtimeGenMaxCreate = value; }
        }

        /// <summary>
        /// Gets the number of times repair has been called on this database.
        /// </summary>
        public int ulRepairCount
        {
            [DebuggerStepThrough]
            get { return this._ulRepairCount; }
            internal set { this._ulRepairCount = value; }
        }

        /// <summary>
        /// Gets the last time that repair was run against this database.
        /// </summary>
        public JET_LOGTIME logtimeRepair
        {
            [DebuggerStepThrough]
            get { return this._logtimeRepair; }
            internal set { this._logtimeRepair = value; }
        }

        /// <summary>
        /// Gets the number of times this database was repaired before the last defrag.
        /// </summary>
        public int ulRepairCountOld
        {
            [DebuggerStepThrough]
            get { return this._ulRepairCountOld; }
            internal set { this._ulRepairCountOld = value; }
        }

        /// <summary>
        /// Gets the number of times a one bit error was successfully fixed.
        /// </summary>
        public int ulECCFixSuccess
        {
            [DebuggerStepThrough]
            get { return this._ulECCFixSuccess; }
            internal set { this._ulECCFixSuccess = value; }
        }

        /// <summary>
        /// Gets the last time a one bit error was successfully fixed.
        /// </summary>
        public JET_LOGTIME logtimeECCFixSuccess
        {
            [DebuggerStepThrough]
            get { return this._logtimeECCFixSuccess; }
            internal set { this._logtimeECCFixSuccess = value; }
        }

        /// <summary>
        /// Gets the number of times a one bit error was successfully fixed before the last repair.
        /// </summary>
        public int ulECCFixSuccessOld
        {
            [DebuggerStepThrough]
            get { return this._ulECCFixSuccessOld; }
            internal set { this._ulECCFixSuccessOld = value; }
        }

        /// <summary>
        /// Gets the number of times an uncorrectable one bit error was encountered.
        /// </summary>
        public int ulECCFixFail
        {
            [DebuggerStepThrough]
            get { return this._ulECCFixFail; }
            internal set { this._ulECCFixFail = value; }
        }

        /// <summary>
        /// Gets the last time an uncorrectable one bit error was encountered.
        /// </summary>
        public JET_LOGTIME logtimeECCFixFail
        {
            [DebuggerStepThrough]
            get { return this._logtimeECCFixFail; }
            internal set { this._logtimeECCFixFail = value; }
        }

        /// <summary>
        /// Gets the number of times an uncorrectable one bit error was encountered.
        /// </summary>
        public int ulECCFixFailOld
        {
            [DebuggerStepThrough]
            get { return this._ulECCFixFailOld; }
            internal set { this._ulECCFixFailOld = value; }
        }

        /// <summary>
        /// Gets the number of times a non-correctable checksum error was found.
        /// </summary>
        public int ulBadChecksum
        {
            [DebuggerStepThrough]
            get { return this._ulBadChecksum; }
            internal set { this._ulBadChecksum = value; }
        }

        /// <summary>
        /// Gets the last time a non-correctable checksum error was found.
        /// </summary>
        public JET_LOGTIME logtimeBadChecksum
        {
            [DebuggerStepThrough]
            get { return this._logtimeBadChecksum; }
            internal set { this._logtimeBadChecksum = value; }
        }

        /// <summary>
        /// Gets the number of times a non-correctable checksum error was found before the last repair.
        /// </summary>
        public int ulBadChecksumOld
        {
            [DebuggerStepThrough]
            get { return this._ulBadChecksumOld; }
            internal set { this._ulBadChecksumOld = value; }
        }

        /// <summary>
        /// Gets the maximum log generation committed to the database. Typically the current log generation.
        /// </summary>
        public int genCommitted
        {
            [DebuggerStepThrough]
            get { return this._genCommitted; }
            internal set { this._genCommitted = value; }
        }

        /// <summary>
        /// Gets information about the last successful copy backup.
        /// </summary>
        public JET_BKINFO bkinfoCopyPrev
        {
            [DebuggerStepThrough]
            get { return this._bkinfoCopyPrev; }
            internal set { this._bkinfoCopyPrev = value; }
        }

        /// <summary>
        /// Gets information about the last successful differential backup. Reset when 
        /// <see cref="bkinfoFullPrev"/> is set.
        /// </summary>
        public JET_BKINFO bkinfoDiffPrev
        {
            [DebuggerStepThrough]
            get { return this._bkinfoDiffPrev; }
            internal set { this._bkinfoDiffPrev = value; }
        }

        /// <summary>
        /// Gets a string representation of this object.
        /// </summary>
        /// <returns>A string representation of this object.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_DBINFOMISC({0})", this._signDb);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            var hashes = new[]
            {
                this._ulVersion,
                this._ulUpdate,
                this._signDb.GetHashCode(),
                this._dbstate.GetHashCode(),
                this._lgposConsistent.GetHashCode(),
                this._logtimeConsistent.GetHashCode(),
                this._logtimeAttach.GetHashCode(),
                this._lgposAttach.GetHashCode(),
                this._logtimeDetach.GetHashCode(),
                this._lgposDetach.GetHashCode(),
                this._signLog.GetHashCode(),
                this._bkinfoFullPrev.GetHashCode(),
                this._bkinfoIncPrev.GetHashCode(),
                this._bkinfoFullCur.GetHashCode(),
                this._fShadowingDisabled.GetHashCode(),
                this._fUpgradeDb.GetHashCode(),
                this._dwMajorVersion,
                this._dwMinorVersion,
                this._dwBuildNumber,
                this._lSPNumber,
                this._cbPageSize,
                this._genMinRequired,
                this._genMaxRequired,
                this._logtimeGenMaxCreate.GetHashCode(),
                this._ulRepairCount,
                this._logtimeRepair.GetHashCode(),
                this._ulRepairCountOld,
                this._ulECCFixSuccess,
                this._logtimeECCFixSuccess.GetHashCode(),
                this._ulECCFixSuccessOld,
                this._ulECCFixFail,
                this._logtimeECCFixFail.GetHashCode(),
                this._ulECCFixFailOld,
                this._ulBadChecksum,
                this._logtimeBadChecksum.GetHashCode(),
                this._ulBadChecksumOld,
                this._genCommitted,
                this._bkinfoCopyPrev.GetHashCode(),
                this._bkinfoDiffPrev.GetHashCode(),
            };

            return Util.CalculateHashCode(hashes);
        }

        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <param name="obj">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>.</param>
        /// <returns>
        /// True if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_DBINFOMISC)obj);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>
        /// True if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        public bool Equals(JET_DBINFOMISC other)
        {
            if (null == other)
            {
                return false;
            }

            return this._ulVersion == other._ulVersion
                   && this._ulUpdate == other._ulUpdate
                   && this._signDb == other._signDb
                   && this._dbstate == other._dbstate
                   && this._lgposConsistent == other._lgposConsistent
                   && this._logtimeConsistent == other._logtimeConsistent
                   && this._logtimeAttach == other._logtimeAttach
                   && this._lgposAttach == other._lgposAttach
                   && this._logtimeDetach == other._logtimeDetach
                   && this._lgposDetach == other._lgposDetach
                   && this._signLog == other._signLog
                   && this._bkinfoFullPrev == other._bkinfoFullPrev
                   && this._bkinfoIncPrev == other._bkinfoIncPrev
                   && this._bkinfoFullCur == other._bkinfoFullCur
                   && this._fShadowingDisabled == other._fShadowingDisabled
                   && this._fUpgradeDb == other._fUpgradeDb
                   && this._dwMajorVersion == other._dwMajorVersion
                   && this._dwMinorVersion == other._dwMinorVersion
                   && this._dwBuildNumber == other._dwBuildNumber
                   && this._lSPNumber == other._lSPNumber
                   && this._cbPageSize == other._cbPageSize
                   && this._genMinRequired == other._genMinRequired
                   && this._genMaxRequired == other._genMaxRequired
                   && this._logtimeGenMaxCreate == other._logtimeGenMaxCreate
                   && this._ulRepairCount == other._ulRepairCount
                   && this._logtimeRepair == other._logtimeRepair
                   && this._ulRepairCountOld == other._ulRepairCountOld
                   && this._ulECCFixSuccess == other._ulECCFixSuccess
                   && this._logtimeECCFixSuccess == other._logtimeECCFixSuccess
                   && this._ulECCFixSuccessOld == other._ulECCFixSuccessOld
                   && this._ulECCFixFail == other._ulECCFixFail
                   && this._logtimeECCFixFail == other._logtimeECCFixFail
                   && this._ulECCFixFailOld == other._ulECCFixFailOld
                   && this._ulBadChecksum == other._ulBadChecksum
                   && this._logtimeBadChecksum == other._logtimeBadChecksum
                   && this._ulBadChecksumOld == other._ulBadChecksumOld
                   && this._genCommitted == other._genCommitted
                   && this._bkinfoCopyPrev == other._bkinfoCopyPrev
                   && this._bkinfoDiffPrev == other._bkinfoDiffPrev;
        }

        /// <summary>
        /// Sets the members of this object from a native object.
        /// </summary>
        /// <param name="native">The native object.</param>
        internal void SetFromNativeDbinfoMisc(ref NATIVE_DBINFOMISC native)
        {
            unchecked
            {
                this._ulVersion = (int)native.ulVersion;
                this._ulUpdate = (int)native.ulUpdate;
                this._signDb = new JET_SIGNATURE(native.signDb);
                this._dbstate = (JET_dbstate)native.dbstate;
                this._lgposConsistent = native.lgposConsistent;
                this._logtimeConsistent = native.logtimeConsistent;
                this._logtimeAttach = native.logtimeAttach;
                this._lgposAttach = native.lgposAttach;
                this._logtimeDetach = native.logtimeDetach;
                this._lgposDetach = native.lgposDetach;
                this._signLog = new JET_SIGNATURE(native.signLog);
                this._bkinfoFullPrev = native.bkinfoFullPrev;
                this._bkinfoIncPrev = native.bkinfoIncPrev;
                this._bkinfoFullCur = native.bkinfoFullCur;
                this._fShadowingDisabled = 0 != native.fShadowingDisabled;
                this._fUpgradeDb = 0 != native.fUpgradeDb;
                this._dwMajorVersion = (int)native.dwMajorVersion;
                this._dwMinorVersion = (int)native.dwMinorVersion;
                this._dwBuildNumber = (int)native.dwBuildNumber;
                this._lSPNumber = (int)native.lSPNumber;
                this._cbPageSize = (int)native.cbPageSize;
            }
        }

        /// <summary>
        /// Sets the members of this object from a native object.
        /// </summary>
        /// <param name="native">The native object.</param>
        internal void SetFromNativeDbinfoMisc(ref NATIVE_DBINFOMISC4 native)
        {
            this.SetFromNativeDbinfoMisc(ref native.dbinfo);

            unchecked
            {
                this._genMinRequired = (int)native.genMinRequired;
                this._genMaxRequired = (int)native.genMaxRequired;
                this._logtimeGenMaxCreate = native.logtimeGenMaxCreate;
                this._ulRepairCount = (int)native.ulRepairCount;
                this._logtimeRepair = native.logtimeRepair;
                this._ulRepairCountOld = (int)native.ulRepairCountOld;
                this._ulECCFixSuccess = (int)native.ulECCFixSuccess;
                this._logtimeECCFixSuccess = native.logtimeECCFixSuccess;
                this._ulECCFixSuccessOld = (int)native.ulECCFixSuccessOld;
                this._ulECCFixFail = (int)native.ulECCFixFail;
                this._logtimeECCFixFail = native.logtimeECCFixFail;
                this._ulECCFixFailOld = (int)native.ulECCFixFailOld;
                this._ulBadChecksum = (int)native.ulBadChecksum;
                this._logtimeBadChecksum = native.logtimeBadChecksum;
                this._ulBadChecksumOld = (int)native.ulBadChecksumOld;
                this._genCommitted = (int)native.genCommitted;
                this._bkinfoCopyPrev = native.bkinfoCopyPrev;
                this._bkinfoDiffPrev = native.bkinfoDiffPrev;
            }
        }

        /// <summary>
        /// Calculates the native version of the structure.
        /// </summary>
        /// <returns>The native version of the structure.</returns>
        internal NATIVE_DBINFOMISC GetNativeDbinfomisc()
        {
            NATIVE_DBINFOMISC native = new NATIVE_DBINFOMISC();

            unchecked
            {
                native.ulVersion = (uint)this._ulVersion;
                native.ulUpdate = (uint)this._ulUpdate;
                native.signDb = this._signDb.GetNativeSignature();
                native.dbstate = (uint)this._dbstate;
                native.lgposConsistent = this._lgposConsistent;
                native.logtimeConsistent = this._logtimeConsistent;
                native.logtimeAttach = this._logtimeAttach;
                native.lgposAttach = this._lgposAttach;
                native.logtimeDetach = this._logtimeDetach;
                native.lgposDetach = this._lgposDetach;
                native.signLog = this._signLog.GetNativeSignature();
                native.bkinfoFullPrev = this._bkinfoFullPrev;
                native.bkinfoIncPrev = this._bkinfoIncPrev;
                native.bkinfoFullCur = this._bkinfoFullCur;
                native.fShadowingDisabled = this._fShadowingDisabled ? 1u : 0u;
                native.fUpgradeDb = this._fUpgradeDb ? 1u : 0u;
                native.dwMajorVersion = (uint)this._dwMajorVersion;
                native.dwMinorVersion = (uint)this._dwMinorVersion;
                native.dwBuildNumber = (uint)this._dwBuildNumber;
                native.lSPNumber = (uint)this._lSPNumber;
                native.cbPageSize = (uint)this._cbPageSize;
            }

            return native;
        }

        /// <summary>
        /// Calculates the native version of the structure.
        /// </summary>
        /// <returns>The native version of the structure.</returns>
        internal NATIVE_DBINFOMISC4 GetNativeDbinfomisc4()
        {
            NATIVE_DBINFOMISC4 native = new NATIVE_DBINFOMISC4();

            native.dbinfo = this.GetNativeDbinfomisc();

            unchecked
            {
                native.genMinRequired = (uint)this._genMinRequired;
                native.genMaxRequired = (uint)this._genMaxRequired;
                native.logtimeGenMaxCreate = this._logtimeGenMaxCreate;
                native.ulRepairCount = (uint)this._ulRepairCount;
                native.logtimeRepair = this._logtimeRepair;
                native.ulRepairCountOld = (uint)this._ulRepairCountOld;
                native.ulECCFixSuccess = (uint)this._ulECCFixSuccess;
                native.logtimeECCFixSuccess = this._logtimeECCFixSuccess;
                native.ulECCFixSuccessOld = (uint)this._ulECCFixSuccessOld;
                native.ulECCFixFail = (uint)this._ulECCFixFail;
                native.logtimeECCFixFail = this._logtimeECCFixFail;
                native.ulECCFixFailOld = (uint)this._ulECCFixFailOld;
                native.ulBadChecksum = (uint)this._ulBadChecksum;
                native.logtimeBadChecksum = this._logtimeBadChecksum;
                native.ulBadChecksumOld = (uint)this._ulBadChecksumOld;
                native.genCommitted = (uint)this._genCommitted;
                native.bkinfoCopyPrev = this._bkinfoCopyPrev;
                native.bkinfoDiffPrev = this._bkinfoDiffPrev;
            }

            return native;
        }
    }
}