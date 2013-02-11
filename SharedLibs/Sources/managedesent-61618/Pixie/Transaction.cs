//-----------------------------------------------------------------------
// <copyright file="Transaction.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Options for transaction commit.
    /// </summary>
    public enum CommitOptions
    {
        /// <summary>
        /// Perform a normal durable commit.
        /// </summary>
        Durable,

        /// <summary>
        /// A fast commit. This does not flush log records to disk.
        /// Committing a transaction this way is faster and preserves
        /// database consistency but some data may be lost in the event
        /// of a crash.
        /// </summary>
        Fast,
    }

    /// <summary>
    /// A class that encapsulates a transaction.
    /// </summary>
    public abstract class Transaction : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the Transaction class.
        /// </summary>
        protected Transaction()
        {
        }

        /// <summary>
        /// Occurs when the transaction rolls back.
        /// </summary>
        public abstract event Action RolledBack;

        /// <summary>
        /// Occurs when the transaction commits.
        /// </summary>
        public abstract event Action Committed;

        /// <summary>
        /// Disposes of an instance of the Transaction class. This will rollback the transaction
        /// if still active.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Commit a transaction. This object must be in a transaction.
        /// </summary>
        public abstract void Commit();

        /// <summary>
        /// Commit a transaction. This object must be in a transaction.
        /// </summary>
        /// <param name="options">
        /// Options for transaction commit.
        /// </param>
        public abstract void Commit(CommitOptions options);

        /// <summary>
        /// Rollback a transaction. This object must be in a transaction.
        /// </summary>
        public abstract void Rollback();

        /// <summary>
        /// Get the CommitTransactionGrbit that should be used for the given options.
        /// </summary>
        /// <param name="options">The commit options.</param>
        /// <returns>The commit grbit for the options.</returns>
        internal static CommitTransactionGrbit GrbitFromOptions(CommitOptions options)
        {
            return CommitOptions.Fast == options ? CommitTransactionGrbit.LazyFlush : CommitTransactionGrbit.None;
        }
   }
}