// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ReadOnlyTransaction.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   The disposable ReadOnlyTransaction wrapper.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Disposable wrapper for a read-only transaction. The Dispose method
    /// will commit the transaction. Unlike the Transaction class this is
    /// a struct, so it isn't as flexible, but it can be faster.
    /// </summary>
    internal struct ReadOnlyTransaction : IDisposable
    {
        /// <summary>
        /// The session that has the transaction.
        /// </summary>
        private readonly JET_SESID sesid;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyTransaction"/> struct.
        /// </summary>
        /// <param name="sesid">
        /// The sesid.
        /// </param>
        public ReadOnlyTransaction(JET_SESID sesid)
        {
            this.sesid = sesid;
            Api.JetBeginTransaction2(this.sesid, BeginTransactionGrbit.ReadOnly);
        }

        /// <summary>
        /// Rollback the transaction if not already committed.
        /// </summary>
        public void Dispose()
        {
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }
    }
}