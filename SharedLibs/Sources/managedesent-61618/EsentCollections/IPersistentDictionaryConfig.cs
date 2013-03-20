// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IPersistentDictionaryConfig.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   An interface for meta-data configuration for the dictionary database.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    /// <summary>
    /// An interface for meta-data configuration for the dictionary database.
    /// </summary>
    internal interface IPersistentDictionaryConfig
    {
        /// <summary>
        /// Gets a string describing the current version of the 
        /// PersistentDictionary.
        /// </summary>
        string Version { get; }

        /// <summary>
        /// Gets the name of the database. The user provides the
        /// directory and the database is always given this name.
        /// </summary>
        string Database { get; }

        /// <summary>
        /// Gets the basename of the logfiles for the instance.
        /// </summary>
        string BaseName { get; }

        /// <summary>
        /// Gets the name of the globals table.
        /// </summary>
        string GlobalsTableName { get; }

        /// <summary>
        /// Gets the name of the version column in the globals table.
        /// </summary>
        string VersionColumnName { get; }

        /// <summary>
        /// Gets the name of the count column in the globals table.
        /// This column tracks the number of items in the collection.
        /// </summary>
        string CountColumnName { get; }

        /// <summary>
        /// Gets the name of the flush column in the globals table.
        /// This column is updated when a Flush operation is performed.
        /// </summary>
        string FlushColumnName { get; }

        /// <summary>
        /// Gets the name of the key type column in the globals table.
        /// This column stores the type of the key in the dictionary.
        /// </summary>
        string KeyTypeColumnName { get; }

        /// <summary>
        /// Gets the name of the value type column in the globals table.
        /// This column stores the type of the value in the dictionary.
        /// </summary>
        string ValueTypeColumnName { get; }

        /// <summary>
        /// Gets the name of the data table.
        /// </summary>
        string DataTableName { get; }

        /// <summary>
        /// Gets the name of the key column in the data table.
        /// This column stores the key of the item.
        /// </summary>
        string KeyColumnName { get; }

        /// <summary>
        /// Gets the name of the value column in the data table.
        /// This column stores the value of the item.
        /// </summary>
        string ValueColumnName { get; }
    }
}