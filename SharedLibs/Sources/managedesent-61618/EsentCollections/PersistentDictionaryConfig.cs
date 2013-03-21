// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryConfig.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//  Code that supports meta-data configuration for the dictionary database.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    /// <summary>
    /// Meta-data configuration for the dictionary database.
    /// </summary>
    internal sealed class PersistentDictionaryConfig : IPersistentDictionaryConfig
    {
        /// <summary>
        /// Gets a string describing the current version of the 
        /// PersistentDictionary.
        /// </summary>
        public string Version
        {
            get
            {
                return "PersistentDictionary 1.0.0";
            }
        }

        /// <summary>
        /// Gets the name of the database. The user provides the
        /// directory and the database is always given this name.
        /// </summary>
        public string Database
        {
            get
            {
                return "PersistentDictionary.edb";
            }
        }

        /// <summary>
        /// Gets the basename of the logfiles for the instance.
        /// </summary>
        public string BaseName
        {
            get
            {
                return "epc";
            }
        }

        /// <summary>
        /// Gets the name of the globals table.
        /// </summary>
        public string GlobalsTableName
        {
            get
            {
                return "Globals";
            }
        }

        /// <summary>
        /// Gets the name of the version column in the globals table.
        /// </summary>
        public string VersionColumnName
        {
            get
            {
                return "Version";
            }
        }

        /// <summary>
        /// Gets the name of the count column in the globals table.
        /// This column tracks the number of items in the collection.
        /// </summary>
        public string CountColumnName
        {
            get
            {
                return "Count";
            }
        }

        /// <summary>
        /// Gets the name of the flush column in the globals table.
        /// This column is updated when a Flush operation is performed.
        /// </summary>
        public string FlushColumnName
        {
            get
            {
                return "Flush";
            }
        }

        /// <summary>
        /// Gets the name of the key type column in the globals table.
        /// This column stores the type of the key in the dictionary.
        /// </summary>
        public string KeyTypeColumnName
        {
            get
            {
                return "KeyType";
            }
        }

        /// <summary>
        /// Gets the name of the value type column in the globals table.
        /// This column stores the type of the value in the dictionary.
        /// </summary>
        public string ValueTypeColumnName
        {
            get
            {
                return "ValueType";
            }
        }

        /// <summary>
        /// Gets the name of the data table.
        /// </summary>
        public string DataTableName
        {
            get
            {
                return "Data";
            }
        }

        /// <summary>
        /// Gets the name of the key column in the data table.
        /// This column stores the key of the item.
        /// </summary>
        public string KeyColumnName
        {
            get
            {
                return "Key";
            }
        }

        /// <summary>
        /// Gets the name of the value column in the data table.
        /// This column stores the value of the item.
        /// </summary>
        public string ValueColumnName
        {
            get
            {
                return "Value";
            }
        }
    }
}