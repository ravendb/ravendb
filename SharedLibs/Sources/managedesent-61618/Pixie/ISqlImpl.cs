//-----------------------------------------------------------------------
// <copyright file="ISqlImpl.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Interface describing methods that implement SQL Commands.
    /// </summary>
    internal interface ISqlImpl : IDisposable
    {
        /// <summary>
        /// CREATE DATABASE
        /// </summary>
        /// <param name="database">Name of the database to create.</param>
        void CreateDatabase(string database);

        /// <summary>
        /// ATTACH DATABASE
        /// </summary>
        /// <param name="database">Name of the database to attach</param>
        void AttachDatabase(string database);

        /// <summary>
        /// DETACH DATABASE
        /// </summary>
        void DetachDatabase();

        /// <summary>
        /// CREATE TABLE
        /// </summary>
        /// <param name="tablename">Name of the table to create.</param>
        /// <param name="columnsToAdd">The columns to add to the table.</param>
        void CreateTable(string tablename, IEnumerable<ColumnDefinition> columnsToAdd);
    
        /// <summary>
        /// BEGIN TRANSACTION
        /// Begin from level 0.
        /// </summary>
        void BeginTransaction();

        /// <summary>
        /// SAVEPOINT
        /// Begin a new subtransaction.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to create.</param>
        void CreateSavepoint(string savepoint);

        /// <summary>
        /// COMMIT TRANSACTION
        /// Commit to level 0.
        /// </summary>
        void CommitTransaction();

        /// <summary>
        /// RELEASE SAVEPOINT
        /// Commit the savepoint.
        /// </summary>
        /// <param name="savepoint">The savepoint to release.</param>
        void CommitSavepoint(string savepoint);

        /// <summary>
        /// ROLLBACK TRANSACTION
        /// Rollback to level 0.
        /// </summary>
        void RollbackTransaction();

        /// <summary>
        /// ROLLBACK TRANSACTION TO SAVEPOINT
        /// Rollback to the specified savepoint.
        /// </summary>
        /// <param name="savepoint">
        /// The savepoint to rollback to. All work done after the creation
        /// of this savepoint will be undone.
        /// </param>
        void RollbackToSavepoint(string savepoint);

        /// <summary>
        /// Insert a new record into a table.
        /// </summary>
        /// <param name="tablename">The table to insert the record into.</param>
        /// <param name="data">The values to insert.</param>
        void InsertRecord(string tablename, IEnumerable<KeyValuePair<string, object>> data);
    }
}