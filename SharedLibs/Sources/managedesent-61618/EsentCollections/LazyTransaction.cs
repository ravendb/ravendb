// --------------------------------------------------------------------------------------------------------------------
// <copyright file="LazyTransaction.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   The disposable LazyTransaction wrapper.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Disposable wrapper for a lazy transaction. The Dispose method
    /// will rollback the transaction if it isn't committed. Unlike
    /// the Transaction class this is a struct, so it isn't as flexible,
    /// but it can be faster.
    /// </summary>
    internal struct LazyTransaction : IDisposable
    {
        /// <summary>
        /// The session that has the transaction.
        /// </summary>
        private readonly JET_SESID sesid;

        /// <summary>
        /// True if we are in a transaction.
        /// </summary>
        private bool inTransaction;

        /// <summary>
        /// Initializes a new instance of the <see cref="LazyTransaction"/> struct.
        /// </summary>
        /// <param name="sesid">
        /// The sesid.
        /// </param>
        public LazyTransaction(JET_SESID sesid)
        {
            this.sesid = sesid;
            Api.JetBeginTransaction(this.sesid);
            this.inTransaction = true;
        }

        /// <summary>
        /// Commit the transaction.
        /// </summary>
        public void Commit()
        {
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            this.inTransaction = false;
        }

        /// <summary>
        /// Rollback the transaction if not already committed.
        /// </summary>
        public void Dispose()
        {
            if (this.inTransaction)
            {
                Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
            }
        }
    }
}