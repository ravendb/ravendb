//-----------------------------------------------------------------------
// <copyright file="Transaction.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// A class that encapsulates a transaction on a JET_SESID.
    /// </summary>
    public class Transaction : EsentResource
    {
        /// <summary>
        /// The underlying JET_SESID.
        /// </summary>
        private readonly JET_SESID sesid;

        /// <summary>
        /// Initializes a new instance of the Transaction class. This automatically
        /// begins a transaction. The transaction will be rolled back if
        /// not explicitly committed.
        /// </summary>
        /// <param name="sesid">The session to start the transaction for.</param>
        public Transaction(JET_SESID sesid)
        {
            this.sesid = sesid;
            this.Begin();
        }

        /// <summary>
        /// Gets a value indicating whether this object is currently in a
        /// transaction.
        /// </summary>
        public bool IsInTransaction
        { 
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.HasResource;
            }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Transaction"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Transaction"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "Transaction (0x{0:x})", this.sesid.Value);
        }

        /// <summary>
        /// Begin a transaction. This object should not currently be
        /// in a transaction.
        /// </summary>
        public void Begin()
        {
            this.CheckObjectIsNotDisposed();
            if (this.IsInTransaction)
            {
                throw new InvalidOperationException("Already in a transaction");
            }

            Api.JetBeginTransaction(this.sesid);
            this.ResourceWasAllocated();
            Debug.Assert(this.IsInTransaction, "Begin finished, but object isn't in a transaction");
        }

        /// <summary>
        /// Commit a transaction. This object should be in a transaction.
        /// </summary>
        /// <param name="grbit">JetCommitTransaction options.</param>
        public void Commit(CommitTransactionGrbit grbit)
        {
            this.CheckObjectIsNotDisposed();
            if (!this.IsInTransaction)
            {
                throw new InvalidOperationException("Not in a transaction");
            }

            Api.JetCommitTransaction(this.sesid, grbit);
            this.ResourceWasReleased();
            Debug.Assert(!this.IsInTransaction, "Commit finished, but object is still in a transaction");
        }

        /// <summary>
        /// Rollback a transaction. This object should be in a transaction.
        /// </summary>
        public void Rollback()
        {
            this.CheckObjectIsNotDisposed();
            if (!this.IsInTransaction)
            {
                throw new InvalidOperationException("Not in a transaction");
            }

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
            this.ResourceWasReleased();
            Debug.Assert(!this.IsInTransaction, "Commit finished, but object is still in a transaction");
        }

        /// <summary>
        /// Called when the transaction is being disposed while active.
        /// This should rollback the transaction.
        /// </summary>
        protected override void ReleaseResource()
        {
            this.Rollback();
        }
    }
}