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
        private readonly KeyColumnConverter<TKey> keyColumnConverter = new KeyColumnConverter<TKey>();

        /// <summary>
        /// Column converter for the value column.
        /// </summary>
        private readonly ColumnConverter<TValue> valueColumnConverter = new ColumnConverter<TValue>();

        /// <summary>
        /// Gets a delegate that can be used to call JetMakeKey with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public KeyColumnConverter<TKey>.MakeKeyDelegate MakeKey
        {
            get
            {
                return this.keyColumnConverter.KeyMaker;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to set the Key column with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter<TKey>.SetColumnDelegate SetKeyColumn
        {
            get
            {
                return this.keyColumnConverter.ColumnSetter;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to set the Value column with an object of
        /// type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter<TValue>.SetColumnDelegate SetValueColumn
        {
            get
            {
                return this.valueColumnConverter.ColumnSetter;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Key column, returning
        /// an object of type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter<TKey>.RetrieveColumnDelegate RetrieveKeyColumn
        {
            get
            {
                return this.keyColumnConverter.ColumnRetriever;
            }
        }

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Value column, returning
        /// an object of type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter<TValue>.RetrieveColumnDelegate RetrieveValueColumn
        {
            get
            {
                return this.valueColumnConverter.ColumnRetriever;
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