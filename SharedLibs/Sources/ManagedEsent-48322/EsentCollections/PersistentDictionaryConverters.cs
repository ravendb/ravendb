// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryConverters.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   A dictionary can contain many types of columns and exposes a strongly typed
//   interface. This code maps between .NET types and functions to set and retrieve
//   data in a PersistentDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Contains methods to set and get data from the ESENT
    /// database.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class PersistentDictionaryConverters<TKey, TValue>
    {
        /// <summary>
        /// Column converter for the key column.
        /// </summary>
        private readonly KeyColumnConverter keyColumnConverter;

        /// <summary>
        /// Column converter for the value column.
        /// </summary>
        private readonly ColumnConverter valueColumnConverter;

        /// <summary>
        /// Initializes a new instance of the PersistentDictionaryConverters
        /// class. This looks up the conversion types for
        /// <typeparamref name="TKey"/> and <typeparamref name="TValue"/>.
        /// </summary>
        public PersistentDictionaryConverters()
        {
            this.keyColumnConverter = new KeyColumnConverter(typeof(TKey));
            this.valueColumnConverter = new ColumnConverter(typeof(TValue));
        }

        /// <summary>
        /// Gets a delegate that can be used to call JetMakeKey with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public KeyColumnConverter.MakeKeyDelegate MakeKey
        {
            get
            {
                return this.keyColumnConverter.MakeKey;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to set the Key column with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter.SetColumnDelegate SetKeyColumn
        {
            get
            {
                return this.keyColumnConverter.SetColumn;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to set the Value column with an object of
        /// type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter.SetColumnDelegate SetValueColumn
        {
            get
            {
                return this.valueColumnConverter.SetColumn;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Key column, returning
        /// an object of type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter.RetrieveColumnDelegate RetrieveKeyColumn
        {
            get
            {
                return this.keyColumnConverter.RetrieveColumn;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Value column, returning
        /// an object of type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter.RetrieveColumnDelegate RetrieveValueColumn
        {
            get
            {
                return this.valueColumnConverter.RetrieveColumn;
            }
        }

        /// <summary>
        /// Gets the JET_coltyp that the key column should have.
        /// </summary>
        public JET_coltyp KeyColtyp
        {
            get
            {
                return this.keyColumnConverter.Coltyp;
            }
        }

        /// <summary>
        /// Gets the JET_coltyp that the value column should have.
        /// </summary>
        public JET_coltyp ValueColtyp
        {
            get
            {
                return this.valueColumnConverter.Coltyp;
            }
        }
    }
}