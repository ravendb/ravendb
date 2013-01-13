//-----------------------------------------------------------------------
// <copyright file="IConnectionManager.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Create connections to databases.
    /// </summary>
    internal interface IConnectionManager
    {
        /// <summary>
        /// Create a new database and return a connection to
        /// the database. The database will be overwritten if
        /// it already exists.
        /// </summary>
        /// <param name="database">The path to the database.</param>
        /// <param name="mode">Creation mode for the database.</param>
        /// <returns>A new connection to the database.</returns>
        Connection CreateDatabase(string database, DatabaseCreationMode mode);

        /// <summary>
        /// Attach an existing database and return a connection to
        /// the database.
        /// </summary>
        /// <param name="database">The path to the database.</param>
        /// <param name="mode">The mode to open the database in.</param>
        /// <returns>A new connection to the database.</returns>
        Connection AttachDatabase(string database, DatabaseOpenMode mode);
    }
}