//-----------------------------------------------------------------------
// <copyright file="Esent.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Options for database creation.
    /// </summary>
    public enum DatabaseCreationMode
    {
        /// <summary>
        /// Default creation mode.
        /// </summary>
        None,

        /// <summary>
        /// Overwrite existing databases.
        /// </summary>
        OverwriteExisting,
    }

    /// <summary>
    /// Options for connection creation.
    /// </summary>
    public enum DatabaseOpenMode
    {
        /// <summary>
        /// Read/Write access to the database.
        /// </summary>
        ReadWrite,

        /// <summary>
        /// Read-only access to the database.
        /// </summary>
        ReadOnly,
    }

    /// <summary>
    /// Static methods to create connections and parsers.
    /// </summary>
    public static class Esent
    {
        /// <summary>
        /// Create a new database and return a connection to it.
        /// </summary>
        /// <param name="database">The database to create.</param>
        /// <param name="mode">Creation mode for the database.</param>
        /// <returns>A connection to the newly created database.</returns>
        public static Connection CreateDatabase(string database, DatabaseCreationMode mode)
        {
            return ConnectionManager().CreateDatabase(database, mode);
        }

        /// <summary>
        /// Create a new database and return a connection to it.
        /// </summary>
        /// <param name="database">The database to create.</param>
        /// <returns>A connection to the newly created database.</returns>
        public static Connection CreateDatabase(string database)
        {
            return Esent.CreateDatabase(database, DatabaseCreationMode.None);
        }

        /// <summary>
        /// Opens an existing database and returns a connection to it.
        /// </summary>
        /// <param name="database">The database to open.</param>
        /// <param name="mode">The mode to open the database in.</param>
        /// <returns>A connection to the database.</returns>
        public static Connection OpenDatabase(string database, DatabaseOpenMode mode)
        {
            return ConnectionManager().AttachDatabase(database, mode);
        }

        /// <summary>
        /// Opens an existing database and returns a connection to it.
        /// </summary>
        /// <param name="database">The database to open.</param>
        /// <returns>A connection to the database.</returns>
        public static Connection OpenDatabase(string database)
        {
            return Esent.OpenDatabase(database, DatabaseOpenMode.ReadWrite);
        }

        /// <summary>
        /// Create an object that can execute SQL commands.
        /// </summary>
        /// <returns>A new SqlConnection object.</returns>
        public static SqlConnection CreateSqlConnection()
        {
            return Dependencies.Container.Resolve<SqlConnection>();
        }

        /// <summary>
        /// Gets the connection manager object to be used for creating and opening databases.
        /// </summary>
        /// <returns>A the connection manager object.</returns>
        private static IConnectionManager ConnectionManager()
        {
            return Dependencies.Container.Resolve<IConnectionManager>();
        }
    }
}
