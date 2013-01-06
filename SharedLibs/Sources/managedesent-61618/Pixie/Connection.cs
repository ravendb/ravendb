//-----------------------------------------------------------------------
// <copyright file="Connection.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A connection to a database.
    /// </summary>
    /// <remarks>
    /// A Connection deals with two concepts:
    ///     - Creating transactions
    ///     - Open/Create/Delete tables
    /// There are two subclasses of Connection, one for read-only access
    /// and one for read-write. The template method pattern is used
    /// to delegate needed decisions to those classes.
    /// </remarks>
    public interface Connection : IDisposable
    {
        /// <summary>
        /// Gets the name of the ESE instance.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the full path of the database.
        /// </summary>
        string Database { get; }

        /// <summary>
        /// Begin a new transaction.
        /// </summary>
        /// <returns>A new transaction object.</returns>
        Transaction BeginTransaction();

        /// <summary>
        /// Perform an operation inside of a transaction. The transaction is durably
        /// committed if the operation completes normally, otherwise it rolls back.
        /// </summary>
        /// <param name="block">The operation to perform.</param>
        /// <returns>The Connection that performed the transaction.</returns>
        Connection UsingTransaction(Action block);

        /// <summary>
        /// Perform an operation inside of a transaction. The transaction is committed
        /// lazy if the operation completes normally, otherwise it rolls back.
        /// </summary>
        /// <param name="block">The operation to perform.</param>
        /// <returns>The Connection that performed the transaction.</returns>
        Connection UsingLazyTransaction(Action block);

        /// <summary>
        /// Create the given table.
        /// </summary>
        /// <param name="tablename">The table to create.</param>
        /// <returns>A new Table object for the table.</returns>
        Table CreateTable(string tablename);

        /// <summary>
        /// Open a table.
        /// </summary>
        /// <param name="tablename">The table to open.</param>
        /// <returns>A new Table object for the table.</returns>
        Table OpenTable(string tablename);
    }
}
