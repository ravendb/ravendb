//-----------------------------------------------------------------------
// <copyright file="Table.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Represents a table in a database.
    /// </summary>
    /// <remarks>
    /// A Table deals with three concepts:
    ///  - Table meta-data: adding/deleting columns and indexes
    ///  - Finding and iterating over records in the table
    ///  - Creating a new record in the table
    /// </remarks>
    public interface Table : IDisposable, IEnumerable<Record>
    {
        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// Gets information about columns in the table.
        /// </summary>
        IDictionary<string, ColumnMetaData> Columns { get; }

        /// <summary>
        /// Create a new record for inserting into the database. Use the Save() method
        /// on the Record to insert the record.
        /// </summary>
        /// <returns>A new record prepared for insert.</returns>
        Record NewRecord();

        /// <summary>
        /// Check to see if the table is empty.
        /// </summary>
        /// <returns>True if the are no (visible) record, False otherwise.</returns>
        bool IsEmpty();

        /// <summary>
        /// Returns the first Record in the table. This throws an exception if the table
        /// is empty.
        /// </summary>
        /// <returns>The first record in the table.</returns>
        Record First();

        /// <summary>
        /// Returns the last Record in the table. This throws an exception if the table
        /// is empty.
        /// </summary>
        /// <returns>The last record in the table.</returns>
        Record Last();

        /// <summary>
        /// Add a new column to the table.
        /// </summary>
        /// <param name="columndef">The column definition.</param>
        /// <returns>The table the column was added to.</returns>
        Table CreateColumn(ColumnDefinition columndef);
    }
}
