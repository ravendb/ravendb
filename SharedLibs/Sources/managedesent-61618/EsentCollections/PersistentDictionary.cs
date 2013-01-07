// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionary.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Implementation of the PersistentDictionary. The dictionary is a collection
//   of persistent keys and values.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Windows7;

    /// <summary>
    /// Represents a collection of persistent keys and values.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public sealed partial class PersistentDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Number of lock objects. Keys are mapped to lock objects using their
        /// hash codes. Making this count a prime number reduces the chance of
        /// collisions.
        /// </summary>
        private const int NumUpdateLocks = 31;

        /// <summary>
        /// The ESENT instance this dictionary uses. An Instance object inherits
        /// from SafeHandle so this instance will be (eventually) terminated even
        /// if the dictionary isn't disposed. 
        /// </summary>
        private readonly Instance instance;

        /// <summary>
        /// An update lock should be taken when the Dictionary is being updated. 
        /// Read operations can proceed without any locks (the cursor cache has
        /// its own lock to control access to the cursors). There are multiple
        /// update locks, which allows multiple writers. When updating a key
        /// take the lock which maps to key.GetHashCode() % updateLocks.Length.
        /// </summary>
        private readonly object[] updateLocks;

        /// <summary>
        /// Methods to set and retrieve data in ESE.
        /// </summary>
        private readonly PersistentDictionaryConverters<TKey, TValue> converters;

        /// <summary>
        /// Meta-data information for the dictionary database.
        /// </summary>
        private readonly IPersistentDictionaryConfig config;

        /// <summary>
        /// Cache of cursors used to access the dictionary.
        /// </summary>
        private readonly PersistentDictionaryCursorCache<TKey, TValue> cursors;

        /// <summary>
        /// Path to the database.
        /// </summary>
        private readonly string databaseDirectory;

        /// <summary>
        /// Path to the database.
        /// </summary>
        private readonly string databasePath;

        /// <summary>
        /// Initializes a new instance of the PersistentDictionary class.
        /// </summary>
        /// <param name="directory">
        /// The directory to create the database in.
        /// </param>
        public PersistentDictionary(string directory)
        {
            if (null == directory)
            {
                throw new ArgumentNullException("directory");
            }

            Globals.Init();
            this.converters = new PersistentDictionaryConverters<TKey, TValue>();
            this.config = new PersistentDictionaryConfig();
            this.databaseDirectory = directory;
            this.databasePath = Path.Combine(directory, this.config.Database);

            this.updateLocks = new object[NumUpdateLocks];
            for (int i = 0; i < this.updateLocks.Length; ++i)
            {
                this.updateLocks[i] = new object();
            }

            this.instance = new Instance(Guid.NewGuid().ToString());            
            this.instance.Parameters.SystemDirectory = directory;
            this.instance.Parameters.LogFileDirectory = directory;
            this.instance.Parameters.TempDirectory = directory;

            // If the database has been moved while inconsistent recovery
            // won't be able to find the database (logfiles contain the
            // absolute path of the referenced database). Set this parameter
            // to indicate a directory which contains any databases that couldn't
            // be found by recovery.
            this.instance.Parameters.AlternateDatabaseRecoveryDirectory = directory;

            this.instance.Parameters.CreatePathIfNotExist = true;
            this.instance.Parameters.BaseName = this.config.BaseName;
            this.instance.Parameters.EnableIndexChecking = false;       // TODO: fix unicode indexes
            this.instance.Parameters.CircularLog = true;
            this.instance.Parameters.CheckpointDepthMax = 64 * 1024 * 1024;
            this.instance.Parameters.LogFileSize = 1024;    // 1MB logs
            this.instance.Parameters.LogBuffers = 1024;     // buffers = 1/2 of logfile
            this.instance.Parameters.MaxTemporaryTables = 0;
            this.instance.Parameters.MaxVerPages = 1024;
            this.instance.Parameters.NoInformationEvent = true;
            this.instance.Parameters.WaypointLatency = 1;
            this.instance.Parameters.MaxSessions = 256;
            this.instance.Parameters.MaxOpenTables = 256;

            InitGrbit grbit = EsentVersion.SupportsWindows7Features
                                  ? Windows7Grbits.ReplayIgnoreLostLogs
                                  : InitGrbit.None;
            this.instance.Init(grbit);

            try
            {
                if (!File.Exists(this.databasePath))
                {
                    this.CreateDatabase(this.databasePath);
                }
                else
                {
                    this.CheckDatabaseMetaData(this.databasePath);
                }

                this.cursors = new PersistentDictionaryCursorCache<TKey, TValue>(
                    this.instance, this.databasePath, this.converters, this.config);
            }
            catch (Exception)
            {
                // We have failed to initialize for some reason. Terminate
                // the instance.
                this.instance.Term();                
                throw;
            }
        }

        /// <summary>
        /// Initializes a new instance of the PersistentDictionary class.
        /// </summary>
        /// <param name="dictionary">
        /// The IDictionary whose contents are copied to the new dictionary.
        /// </param>
        /// <param name="directory">
        /// The directory to create the database in.
        /// </param>
        public PersistentDictionary(IEnumerable<KeyValuePair<TKey, TValue>> dictionary, string directory) : this(directory)
        {
            try
            {
                if (null == dictionary)
                {
                    throw new ArgumentNullException("dictionary");
                }

                foreach (KeyValuePair<TKey, TValue> item in dictionary)
                {
                    this.Add(item);
                }
            }
            catch (Exception)
            {
                // We have failed to copy the dictionary. Terminate the instance.
                this.instance.Term();
                throw;
            }
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <value>
        /// The number of elements contained in the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </value>
        public int Count
        {
            get
            {
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    return cursor.RetrieveCount();
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="PersistentDictionary{TKey,TValue}"/> is read-only.
        /// </summary>
        /// <value>
        /// True if the <see cref="PersistentDictionary{TKey,TValue}"/> is read-only; otherwise, false.
        /// </value>
        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Gets an <see cref="ICollection"/> containing the keys of the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <returns>
        /// An <see cref="PersistentDictionaryKeyCollection{TKey,TValue}"/> containing the keys of the object that implements <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </returns>
        ICollection<TKey> IDictionary<TKey, TValue>.Keys
        {
            get
            {
                return this.Keys;
            }
        }

        /// <summary>
        /// Gets an <see cref="ICollection"/> containing the keys of the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <value>
        /// An <see cref="PersistentDictionaryKeyCollection{TKey,TValue}"/> containing the keys of the object that implements <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </value>
        public PersistentDictionaryKeyCollection<TKey, TValue> Keys
        {
            get
            {
                return new PersistentDictionaryKeyCollection<TKey, TValue>(this);
            }
        }

        /// <summary>
        /// Gets an <see cref="ICollection"/> containing the values in the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <value>
        /// An <see cref="PersistentDictionary{TKey,TValue}"/> containing the values in the object that implements <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </value>
        ICollection<TValue> IDictionary<TKey, TValue>.Values
        {
            get
            {
                return this.Values;
            }
        }

        /// <summary>
        /// Gets an <see cref="ICollection"/> containing the values in the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <value>
        /// An <see cref="PersistentDictionary{TKey,TValue}"/> containing the values in the object that implements <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </value>
        public PersistentDictionaryValueCollection<TKey, TValue> Values
        {
            get
            {
                return new PersistentDictionaryValueCollection<TKey, TValue>(this);
            }
        }

        /// <summary>
        /// Gets the path of the directory that contains the dictionary database.
        /// The database consists of a set of files found in the directory.
        /// </summary>
        /// <value>
        /// The path of the directory that contains the dictionary database.
        /// </value>
        public string Database
        {
            get
            {
                return this.databaseDirectory;
            }
        }

        /// <summary>
        /// Gets or sets the element with the specified key.
        /// </summary>
        /// <returns>
        /// The element with the specified key.
        /// </returns>
        /// <param name="key">The key of the element to get or set.</param>
        /// <exception cref="T:System.Collections.Generic.KeyNotFoundException">
        /// The property is retrieved and <paramref name="key"/> is not found.
        /// </exception>
        public TValue this[TKey key]
        {
            get
            {
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    using (var transaction = cursor.BeginReadOnlyTransaction())
                    {
                        cursor.SeekWithKeyNotFoundException(key);
                        var value = cursor.RetrieveCurrentValue();
                        return value;
                    }
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }
            }

            set
            {
                lock (this.LockObject(key))
                {
                    PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                    try
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            if (cursor.TrySeek(key))
                            {
                                cursor.ReplaceCurrentValue(value);
                            }
                            else
                            {
                                cursor.Insert(new KeyValuePair<TKey, TValue>(key, value));
                            }

                            transaction.Commit();
                        }
                    }
                    finally
                    {
                        this.cursors.FreeCursor(cursor);
                    }
                }
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> 
        /// that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new PersistentDictionaryEnumerator<TKey, TValue, KeyValuePair<TKey, TValue>>(
                this, KeyRange<TKey>.OpenRange, c => c.RetrieveCurrent(), x => true);
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.IEnumerator"/>
        /// object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <returns>
        /// True if <paramref name="item"/> was successfully removed from the <see cref="PersistentDictionary{TKey,TValue}"/>;
        /// otherwise, false. This method also returns false if <paramref name="item"/> is not found in the original
        /// <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </returns>
        /// <param name="item">The object to remove from the <see cref="PersistentDictionary{TKey,TValue}"/>.</param>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            lock (this.LockObject(item.Key))
            {            
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    // Having the update lock means the record can't be
                    // deleted after we seek to it.
                    if (cursor.TrySeek(item.Key) && cursor.RetrieveCurrentValue().Equals(item.Value))
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            cursor.DeleteCurrent();
                            transaction.Commit();
                            return true;
                        }
                    }

                    return false;
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }
            }
        }

        /// <summary>
        /// Adds an item to the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <param name="item">The object to add to the <see cref="PersistentDictionary{TKey,TValue}"/>.</param>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            lock (this.LockObject(item.Key))
            {
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    using (var transaction = cursor.BeginLazyTransaction())
                    {
                        if (cursor.TrySeek(item.Key))
                        {
                            throw new ArgumentException("An item with this key already exists", "key");
                        }

                        cursor.Insert(item);
                        transaction.Commit();
                    }
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }    
            }
        }

        /// <summary>
        /// Determines whether the <see cref="PersistentDictionary{TKey,TValue}"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// True if <paramref name="item"/> is found in the
        /// <see cref="PersistentDictionary{TKey,TValue}"/>;
        /// otherwise, false.
        /// </returns>
        /// <param name="item">
        /// The object to locate in the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </param>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
            try
            {
                // Start a transaction here to avoid the case where the record
                // is deleted after we seek to it.
                using (var transaction = cursor.BeginReadOnlyTransaction())
                {
                    bool isPresent = cursor.TrySeek(item.Key)
                                     && Compare.AreEqual(item.Value, cursor.RetrieveCurrentValue());
                    return isPresent;
                }
            }
            finally
            {
                this.cursors.FreeCursor(cursor);
            }
        }

        /// <summary>
        /// Copies the elements of the <see cref="PersistentDictionary{TKey,TValue}"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        /// <param name="array">
        /// The one-dimensional <see cref="T:System.Array"/> that is the destination
        /// of the elements copied from <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// The <see cref="T:System.Array"/> must have zero-based indexing.</param>
        /// <param name="arrayIndex">
        /// The zero-based index in <paramref name="array"/> at which copying begins.
        /// </param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="array"/> is null.</exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is less than 0.</exception>
        /// <exception cref="T:System.ArgumentException">
        /// <paramref name="arrayIndex"/> is equal to or greater than the length of <paramref name="array"/>.
        /// -or-The number of elements in the source <see cref="PersistentDictionary{TKey,TValue}"/> is greater
        /// than the available space from <paramref name="arrayIndex"/> to the end of the destination <paramref name="array"/>.
        /// </exception>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            Copy.CopyTo(this, array, arrayIndex);
        }

        /// <summary>
        /// Removes all items from the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        public void Clear()
        {
            // We will be deleting all items so take all the update locks
            foreach (object lockObject in this.updateLocks)
            {
                Monitor.Enter(lockObject);
            }

            try
            {
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    cursor.MoveBeforeFirst();
                    while (cursor.TryMoveNext())
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            cursor.DeleteCurrent();
                            transaction.Commit();
                        }
                    }
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }
            }
            finally
            {
                // Remember to unlock everything
                foreach (object lockObject in this.updateLocks)
                {
                    Monitor.Exit(lockObject);
                }                
            }
        }

        /// <summary>
        /// Determines whether the <see cref="PersistentDictionary{TKey,TValue}"/> contains an element with the specified key.
        /// </summary>
        /// <returns>
        /// True if the <see cref="PersistentDictionary{TKey,TValue}"/> contains an element with the key; otherwise, false.
        /// </returns>
        /// <param name="key">The key to locate in the <see cref="PersistentDictionary{TKey,TValue}"/>.</param>
        public bool ContainsKey(TKey key)
        {
            PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
            try
            {
                return cursor.TrySeek(key);
            }
            finally
            {
                this.cursors.FreeCursor(cursor);
            }
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <param name="key">The object to use as the key of the element to add.</param>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.ArgumentException">An element with the same key already exists in the <see cref="PersistentDictionary{TKey,TValue}"/>.</exception>
        public void Add(TKey key, TValue value)
        {
            this.Add(new KeyValuePair<TKey, TValue>(key, value));
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </summary>
        /// <returns>
        /// True if the element is successfully removed; otherwise, false. This method also returns false if
        /// <paramref name="key"/> was not found in the original <see cref="PersistentDictionary{TKey,TValue}"/>.
        /// </returns>
        /// <param name="key">The key of the element to remove.</param>
        public bool Remove(TKey key)
        {
            lock (this.LockObject(key))
            {
                PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
                try
                {
                    if (cursor.TrySeek(key))
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            cursor.DeleteCurrent();
                            transaction.Commit();
                            return true;
                        }
                    }

                    return false;
                }
                finally
                {
                    this.cursors.FreeCursor(cursor);
                }
            }
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <returns>
        /// True if the object that implements <see cref="PersistentDictionary{TKey,TValue}"/>
        /// contains an element with the specified key; otherwise, false.
        /// </returns>
        /// <param name="key">
        /// The key whose value to get.</param>
        /// <param name="value">When this method returns, the value associated
        /// with the specified key, if the key is found; otherwise, the default
        /// value for the type of the <paramref name="value"/> parameter. This
        /// parameter is passed uninitialized.
        /// </param>
        public bool TryGetValue(TKey key, out TValue value)
        {
            TValue retrievedValue = default(TValue);
            PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
            try
            {
                // Start a transaction so the record can't be deleted after
                // we seek to it.
                bool isPresent = false;
                using (var transaction = cursor.BeginReadOnlyTransaction())
                {
                    if (cursor.TrySeek(key))
                    {
                        retrievedValue = cursor.RetrieveCurrentValue();
                        isPresent = true;
                    }
                }

                value = retrievedValue;
                return isPresent;
            }
            finally
            {
                this.cursors.FreeCursor(cursor);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.cursors.Dispose();
            this.instance.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Determines whether the <see cref="PersistentDictionary{TKey,TValue}"/> contains an element with the specified value.
        /// </summary>
        /// <remarks>
        /// This method requires a complete enumeration of all items in the dictionary so it can be much slower than
        /// <see cref="ContainsKey"/>.
        /// </remarks>
        /// <returns>
        /// True if the <see cref="PersistentDictionary{TKey,TValue}"/> contains an element with the value; otherwise, false.
        /// </returns>
        /// <param name="value">The value to locate in the <see cref="PersistentDictionary{TKey,TValue}"/>.</param>
        public bool ContainsValue(TValue value)
        {
            return this.Values.Contains(value);
        }

        /// <summary>
        /// Force all changes made to this dictionary to be written to disk.
        /// </summary>
        public void Flush()
        {
            PersistentDictionaryCursor<TKey, TValue> cursor = this.cursors.GetCursor();
            try
            {
                cursor.Flush();
            }
            finally
            {
                this.cursors.FreeCursor(cursor);
            }
        }

        /// <summary>
        /// Opens a cursor on the PersistentDictionary. Used by enumerators.
        /// </summary>
        /// <returns>
        /// A new cursor that can be used to enumerate the PersistentDictionary.
        /// </returns>
        internal PersistentDictionaryCursor<TKey, TValue> GetCursor()
        {
            return this.cursors.GetCursor();
        }

        /// <summary>
        /// Opens a cursor on the PersistentDictionary. Used by enumerators.
        /// </summary>
        /// <param name="cursor">
        /// The cursor being freed.
        /// </param>
        internal void FreeCursor(PersistentDictionaryCursor<TKey, TValue> cursor)
        {
            this.cursors.FreeCursor(cursor);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the values.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the values.
        /// </returns>
        internal IEnumerator<TValue> GetValueEnumerator()
        {
            return new PersistentDictionaryEnumerator<TKey, TValue, TValue>(
                this, KeyRange<TKey>.OpenRange, c => c.RetrieveCurrentValue(), x => true);
        }

        /// <summary>
        /// Determine if the given column can be compressed.
        /// </summary>
        /// <param name="columndef">The definition of the column.</param>
        /// <returns>True if the column can be compressed.</returns>
        private static bool ColumnCanBeCompressed(JET_COLUMNDEF columndef)
        {
            return EsentVersion.SupportsWindows7Features
                   && (JET_coltyp.LongText == columndef.coltyp || JET_coltyp.LongBinary == columndef.coltyp);
        }

        /// <summary>
        /// Check the database meta-data. This makes sure the tables and columns exist and
        /// checks the type of the database.
        /// </summary>
        /// <param name="database">The database to check.</param>
        private void CheckDatabaseMetaData(string database)
        {
            using (var session = new Session(this.instance))
            {
                JET_DBID dbid;
                JET_TABLEID tableid;

                Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                Api.JetOpenDatabase(session, database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                // Globals table
                Api.JetOpenTable(session, dbid, this.config.GlobalsTableName, null, 0, OpenTableGrbit.None, out tableid);
                Api.GetTableColumnid(session, tableid, this.config.CountColumnName);
                Api.GetTableColumnid(session, tableid, this.config.FlushColumnName);
                var keyTypeColumnid = Api.GetTableColumnid(session, tableid, this.config.KeyTypeColumnName);
                var valueTypeColumnid = Api.GetTableColumnid(session, tableid, this.config.ValueTypeColumnName);
                if (!Api.TryMoveFirst(session, tableid))
                {
                    throw new InvalidDataException("globals table is empty");
                }

                var keyType = Api.DeserializeObjectFromColumn(session, tableid, keyTypeColumnid);
                var valueType = Api.DeserializeObjectFromColumn(session, tableid, valueTypeColumnid);
                if (keyType != typeof(TKey) || valueType != typeof(TValue))
                {
                    var error = String.Format(
                        CultureInfo.InvariantCulture,
                        "Database is of type <{0}, {1}>, not <{2}, {3}>",
                        keyType,
                        valueType,
                        typeof(TKey),
                        typeof(TValue));
                    throw new ArgumentException(error);
                }

                Api.JetCloseTable(session, tableid);

                // Data table
                Api.JetOpenTable(session, dbid, this.config.DataTableName, null, 0, OpenTableGrbit.None, out tableid);
                Api.GetTableColumnid(session, tableid, this.config.KeyColumnName);
                Api.GetTableColumnid(session, tableid, this.config.ValueColumnName);
                Api.JetCloseTable(session, tableid);
            }
        }

        /// <summary>
        /// Create the database.
        /// </summary>
        /// <param name="database">The name of the database to create.</param>
        private void CreateDatabase(string database)
        {
            using (var session = new Session(this.instance))
            {
                JET_DBID dbid;
                Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                try
                {
                    using (var transaction = new Transaction(session))
                    {
                        this.CreateGlobalsTable(session, dbid);
                        this.CreateDataTable(session, dbid);
                        transaction.Commit(CommitTransactionGrbit.None);
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                        Api.JetDetachDatabase(session, database);
                    }
                }
                catch
                {
                    // Delete the partially constructed database
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    Api.JetDetachDatabase(session, database);
                    File.Delete(database);
                    throw;
                }
            }
        }

        /// <summary>
        /// Create the globals table.
        /// </summary>
        /// <param name="session">The session to use.</param>
        /// <param name="dbid">The database to create the table in.</param>
        private void CreateGlobalsTable(Session session, JET_DBID dbid)
        {
            JET_TABLEID tableid;
            JET_COLUMNID versionColumnid;
            JET_COLUMNID countColumnid;
            JET_COLUMNID keyTypeColumnid;
            JET_COLUMNID valueTypeColumnid;

            Api.JetCreateTable(session, dbid, this.config.GlobalsTableName, 1, 100, out tableid);
            Api.JetAddColumn(
                session,
                tableid,
                this.config.VersionColumnName,
                new JET_COLUMNDEF { coltyp = JET_coltyp.LongText },
                null,
                0,
                out versionColumnid);

            byte[] defaultValue = BitConverter.GetBytes(0);

            Api.JetAddColumn(
                session,
                tableid,
                this.config.CountColumnName,
                new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate },
                defaultValue,
                defaultValue.Length,
                out countColumnid);

            Api.JetAddColumn(
                session,
                tableid,
                this.config.FlushColumnName,
                new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate },
                defaultValue,
                defaultValue.Length,
                out countColumnid);

            Api.JetAddColumn(
                session,
                tableid,
                this.config.KeyTypeColumnName,
                new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary },
                null,
                0,
                out keyTypeColumnid);

            Api.JetAddColumn(
                session,
                tableid,
                this.config.ValueTypeColumnName,
                new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary },
                null,
                0,
                out valueTypeColumnid);

            using (var update = new Update(session, tableid, JET_prep.Insert))
            {
                Api.SerializeObjectToColumn(session, tableid, keyTypeColumnid, typeof(TKey));
                Api.SerializeObjectToColumn(session, tableid, valueTypeColumnid, typeof(TValue));
                Api.SetColumn(session, tableid, versionColumnid, this.config.Version, Encoding.Unicode);
                update.Save();
            }

            Api.JetCloseTable(session, tableid);
        }

        /// <summary>
        /// Create the data table.
        /// </summary>
        /// <param name="session">The session to use.</param>
        /// <param name="dbid">The database to create the table in.</param>
        private void CreateDataTable(Session session, JET_DBID dbid)
        {
            JET_TABLEID tableid;
            JET_COLUMNID keyColumnid;
            JET_COLUMNID valueColumnid;

            Api.JetCreateTable(session, dbid, this.config.DataTableName, 128, 100, out tableid);
            var columndef = new JET_COLUMNDEF { coltyp = this.converters.KeyColtyp, cp = JET_CP.Unicode, grbit = ColumndefGrbit.None };
            if (ColumnCanBeCompressed(columndef))
            {
                columndef.grbit |= Windows7Grbits.ColumnCompressed;
            }

            Api.JetAddColumn(
                session,
                tableid,
                this.config.KeyColumnName,
                columndef,
                null,
                0,
                out keyColumnid);

            columndef = new JET_COLUMNDEF { coltyp = this.converters.ValueColtyp, cp = JET_CP.Unicode, grbit = ColumndefGrbit.None };
            if (ColumnCanBeCompressed(columndef))
            {
                columndef.grbit |= Windows7Grbits.ColumnCompressed;
            }

            Api.JetAddColumn(
                session,
                tableid,
                this.config.ValueColumnName,
                columndef,
                null,
                0,
                out valueColumnid);

            string indexKey = String.Format(CultureInfo.InvariantCulture, "+{0}\0\0", this.config.KeyColumnName);
            var indexcreates = new[]
            {
                new JET_INDEXCREATE
                {
                    cbKeyMost = SystemParameters.KeyMost,
                    grbit = CreateIndexGrbit.IndexPrimary,
                    szIndexName = "primary",
                    szKey = indexKey,
                    cbKey = indexKey.Length,
                    pidxUnicode = new JET_UNICODEINDEX
                    {
                        lcid = CultureInfo.CurrentCulture.LCID,
                        dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.None),
                    },
                },
            };
            Api.JetCreateIndex2(session, tableid, indexcreates, indexcreates.Length);

            Api.JetCloseTable(session, tableid);
        }

        /// <summary>
        /// Gets an object used to lock updates to the key.
        /// </summary>
        /// <param name="key">The key to be locked.</param>
        /// <returns>
        /// An object that should be locked when the key is updated.
        /// </returns>
        private object LockObject(TKey key)
        {
            if (null == key)
            {
                return this.updateLocks[0];
            }
            
            // Remember: hash codes can be negative, and we can't negate Int32.MinValue.
            uint hash = unchecked((uint)key.GetHashCode());
            hash %= checked((uint)this.updateLocks.Length);

            return this.updateLocks[checked((int)hash)];
        }
    }
}