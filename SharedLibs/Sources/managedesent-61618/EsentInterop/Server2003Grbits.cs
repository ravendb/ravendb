//-----------------------------------------------------------------------
// <copyright file="Server2003Grbits.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Server2003
{
    using System;

    /// <summary>
    /// Options for <see cref="Server2003Api.JetOSSnapshotAbort"/>.
    /// </summary>
    public enum SnapshotAbortGrbit
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,    
    }

    /// <summary>
    /// Options for <see cref="Server2003Api.JetUpdate2"/>.
    /// </summary>
    public enum UpdateGrbit
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,

        /// <summary>
        /// This flag causes the update to return an error if the update would
        /// not have been possible in the Windows 2000 version of ESE, which
        /// enforced a smaller maximum number of multi-valued column instances
        /// in each record than later versions of ESE. This is important only
        /// for applications that wish to replicate data between applications
        /// hosted on Windows 2000 and applications hosted on Windows 
        /// 2003, or later versions of ESE. It should not be necessary for most
        /// applications.
        /// </summary>
        [Obsolete("Only needed for legacy replication applications.")]
        CheckESE97Compatibility = 0x1,
    }

    /// <summary>
    /// Grbits that have been added to the Windows Server 2003 version of ESENT.
    /// </summary>
    public static class Server2003Grbits
    {
        /// <summary>
        /// This option requests that the temporary table only be created if the
        /// temporary table manager can use the implementation optimized for
        /// intermediate query results. If any characteristic of the temporary
        /// table would prevent the use of this optimization then the operation
        /// will fail with JET_errCannotMaterializeForwardOnlySort. A side effect
        /// of this option is to allow the temporary table to contain records
        /// with duplicate index keys. See <see cref="TempTableGrbit.Unique"/>
        /// for more information.
        /// </summary>        
        public const TempTableGrbit ForwardOnly = (TempTableGrbit)0x40;

        /// <summary>
        /// If a given column is not present in the record and it has a user
        /// defined default value then no column value will be returned.
        /// This option will prevent the callback that computes the user defined
        /// default value for the column from being called when enumerating
        /// the values for that column.
        /// </summary>
        /// <remarks>
        /// This option is only available for Windows Server 2003 SP1 and later
        /// operating systems.
        /// </remarks>
        public const EnumerateColumnsGrbit EnumerateIgnoreUserDefinedDefault = (EnumerateColumnsGrbit)0x00100000;

        /// <summary>
        /// All transactions previously committed by any session that have not
        /// yet been flushed to the transaction log file will be flushed immediately.
        /// This API will wait until the transactions have been flushed before
        /// returning to the caller. This option may be used even if the session
        /// is not currently in a transaction. This option cannot be used in
        /// combination with any other option.
        /// </summary>
        public const CommitTransactionGrbit WaitAllLevel0Commit = (CommitTransactionGrbit)0x8;
    }
}