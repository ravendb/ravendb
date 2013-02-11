//-----------------------------------------------------------------------
// <copyright file="Record.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A table record.
    /// </summary>
    public interface Record : IDisposable
    {
        /// <summary>
        /// Gets or sets the columns in the record.
        /// </summary>
        /// <param name="column">Name of the column.</param>
        /// <returns>The column as an object.</returns>
        object this[string column]
        { 
            get;
            set;
        }

        /// <summary>
        /// Save changes in the current update.
        /// </summary>
        void Save();

        /// <summary>
        /// Delete the current record. This disposes the current object.
        /// </summary>
        void Delete();

        /// <summary>
        /// Cancel changes in the current update.
        /// </summary>
        void Cancel();

        /// <summary>
        /// Set a column in the record.
        /// </summary>
        /// <param name="column">The column to set.</param>
        /// <param name="value">The value to set.</param>
        /// <returns>The record the column was set on.</returns>
        Record SetColumn(string column, object value);
    }
}