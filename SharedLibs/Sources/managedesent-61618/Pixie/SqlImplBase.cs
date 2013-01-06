//-----------------------------------------------------------------------
// <copyright file="SqlImplBase.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// This class exposes methods that implement SQL Commands
    /// </summary>
    internal class SqlImplBase : ISqlImpl
    {
        /// <summary>
        /// Savepoints. These are in a stack from most recent to oldest.
        /// </summary>
        private readonly Stack<Savepoint> savepoints;

        /// <summary>
        /// The maximum number of outstanding savepoints.
        /// </summary>
        private const int MaxSavepoints = 5;

        /// <summary>
        /// Controls the tracing of the object.
        /// </summary>
        private readonly Tracer tracer;

        /// <summary>
        /// The database connection for this parser.
        /// </summary>
        private Connection connection;

        /// <summary>
        /// The outer (level-0) transaction.
        /// </summary>
        private Transaction outerTransaction;

        /// <summary>
        /// The last table that was used. Cached in case we use it again.
        /// </summary>
        private Table cachedTable;

        /// <summary>
        /// Initializes a new instance of the SqlImplBase class.
        /// </summary>
        public SqlImplBase()
        {
            this.tracer = new Tracer("SqlImpl", "Execute SQL commands", "SQL executor");
            this.savepoints = new Stack<Savepoint>(MaxSavepoints);
        }

        /// <summary>
        /// Gets or sets the current database connection.
        /// </summary>
        protected Connection Connection
        {
            get
            {
                if (null == this.connection)
                {
                    this.SqlExecutionError("No database is currently attached");
                }

                return this.connection;
            }

            set
            {
                if (null != this.connection && null != value)
                {
                    this.Tracer.TraceError("still connected to database '{0}'", this.Connection.Database);
                    throw new InvalidOperationException("still connected to a database");
                }

                this.connection = value;
            }
        }

        /// <summary>
        /// Gets the Tracer object for this Parser.
        /// </summary>
        private Tracer Tracer
        {
            get
            {
                return this.tracer;
            }
        }

        /// <summary>
        /// Called when the object is disposed. Closes the connection.
        /// </summary>
        public virtual void Dispose()
        {
            this.CloseConnection();
        }

        /// <summary>
        /// CREATE DATABASE
        /// </summary>
        /// <param name="database">Name of the database to create.</param>
        public virtual void CreateDatabase(string database)
        {
            this.Tracer.TraceVerbose("creating database '{0}'", database);
            if (null != this.connection)
            {
                this.Tracer.TraceWarning("automatically closing connection to '{0}'", this.Connection.Database);
            }

            this.CloseConnection();
            this.Connection = Esent.CreateDatabase(database, DatabaseCreationMode.None);
            Debug.Assert(null != this.connection, "Should have a database connected");
        }

        /// <summary>
        /// ATTACH DATABASE
        /// </summary>
        /// <param name="database">Name of the database to attach</param>
        public virtual void AttachDatabase(string database)
        {
            this.Tracer.TraceVerbose("attaching database '{0}'", database);
            if (null != this.connection)
            {
                this.Tracer.TraceWarning("automatically closing connection to '{0}'", this.Connection.Database);
            }

            this.CloseConnection();
            this.Connection = Esent.OpenDatabase(database, DatabaseOpenMode.ReadWrite);
            Debug.Assert(null != this.connection, "Should have a database connected");
        }

        /// <summary>
        /// DETACH DATABASE
        /// </summary>
        public virtual void DetachDatabase()
        {
            this.Tracer.TraceVerbose("detaching database");
            if (null == this.connection)
            {
                this.Tracer.TraceWarning("DETACH DATABASE called with no database attached");
            }

            this.CloseConnection();
            Debug.Assert(null == this.connection, "Should not have a database connected");
        }

        /// <summary>
        /// CREATE TABLE
        /// </summary>
        /// <param name="tablename">Name of the table to create.</param>
        /// <param name="columnsToAdd">The columns to add to the table.</param>
        public virtual void CreateTable(string tablename, IEnumerable<ColumnDefinition> columnsToAdd)
        {
            this.Tracer.TraceInfo("creating table '{0}'", tablename);
            using (Transaction trx = this.Connection.BeginTransaction())
            {
                using (Table table = this.Connection.CreateTable(tablename))
                {
                    foreach (ColumnDefinition column in columnsToAdd)
                    {
                        this.Tracer.TraceVerbose("adding column '{0}'", column.Name);
                        table.CreateColumn(column);
                    }
                }

                trx.Commit();
            }
        }

        /// <summary>
        /// BEGIN TRANSACTION
        /// Begin from level 0.
        /// </summary>
        public virtual void BeginTransaction()
        {
            this.Tracer.TraceVerbose("begin transaction");
            if (null != this.outerTransaction)
            {
                this.SqlExecutionError("Invalid BEGIN TRANSACTION: already in a transaction");
            }

            this.outerTransaction = this.Connection.BeginTransaction();
        }

        /// <summary>
        /// SAVEPOINT
        /// Begin a new subtransaction.
        /// </summary>
        /// <param name="savepoint">The name of the savepoint to create.</param>
        public virtual void CreateSavepoint(string savepoint)
        {
            this.Tracer.TraceVerbose("create savepoint '{0}'", savepoint);
            if (null == this.outerTransaction)
            {
                this.SqlExecutionError("Invalid SAVEPOINT: not in a transaction");
            }

            Transaction transaction = this.Connection.BeginTransaction();
            this.savepoints.Push(new Savepoint { Name = savepoint, Transaction = transaction });
        }

        /// <summary>
        /// COMMIT TRANSACTION
        /// Commit to level 0.
        /// </summary>
        public virtual void CommitTransaction()
        {
            this.Tracer.TraceVerbose("commit transaction");
            if (null == this.outerTransaction)
            {
                this.SqlExecutionError("Invalid COMMIT TRANSACTION: not in a transaction");
            }

            this.CloseCachedTable();
            this.outerTransaction.Commit();
            this.savepoints.Clear();
            this.outerTransaction = null;
        }

        /// <summary>
        /// RELEASE SAVEPOINT
        /// Commit the savepoint.
        /// </summary>
        /// <param name="savepoint">The savepoint to release.</param>
        public virtual void CommitSavepoint(string savepoint)
        {
            this.Tracer.TraceVerbose("commit savepoint '{0}'", savepoint);
        }

        /// <summary>
        /// ROLLBACK TRANSACTION
        /// Rollback to level 0.
        /// </summary>
        public virtual void RollbackTransaction()
        {
            this.Tracer.TraceVerbose("rollback transaction");
            if (null == this.outerTransaction)
            {
                this.SqlExecutionError("Invalid ROLLBACK TRANSACTION: not in a transaction");
            }

            this.CloseCachedTable();
            this.outerTransaction.Rollback();
            this.savepoints.Clear();
            this.outerTransaction = null;
        }

        /// <summary>
        /// ROLLBACK TRANSACTION TO SAVEPOINT
        /// Rollback to the specified savepoint.
        /// </summary>
        /// <param name="savepoint">
        /// The savepoint to rollback to. All work done after the creation
        /// of this savepoint will be undone.
        /// </param>
        public virtual void RollbackToSavepoint(string savepoint)
        {
            this.Tracer.TraceVerbose("rollback transaction to savepoint '{0}'", savepoint);
        }

        /// <summary>
        /// Insert a new record into a table.
        /// </summary>
        /// <param name="tablename">The table to insert the record into.</param>
        /// <param name="data">The values to insert.</param>
        public virtual void InsertRecord(string tablename, IEnumerable<KeyValuePair<string, object>> data)
        {
            this.Tracer.TraceVerbose("insert into table {0}", tablename);

            // Start a transaction if necessary.
            if (null == this.outerTransaction)
            {
                this.Tracer.TraceVerbose("starting a transaction", tablename);
                using (Transaction transaction = this.Connection.BeginTransaction())
                {
                    this.InsertRecordImpl(tablename, data);
                    ////this.CloseCachedTable();
                    transaction.Commit();
                }
            }
            else
            {
                this.InsertRecordImpl(tablename, data);
            }
        }

        /// <summary>
        /// Insert a new record into a table.
        /// </summary>
        /// <param name="tablename">The table to insert the record into.</param>
        /// <param name="data">The values to insert.</param>
        private void InsertRecordImpl(string tablename, IEnumerable<KeyValuePair<string, object>> data)
        {
            Table table = this.OpenTable(tablename);
            Record record = table.NewRecord();
            foreach (KeyValuePair<string, object> column in data)
            {
                record[column.Key] = column.Value;
            }

            record.Save();
        }

        /// <summary>
        /// Opens the table. This can cache an open table.
        /// </summary>
        /// <param name="tablename">The name of the table.</param>
        /// <returns>An opened table.</returns>
        private Table OpenTable(string tablename)
        {
            if (null != this.cachedTable && String.Equals(tablename, this.cachedTable.TableName))
            {
                return this.cachedTable;
            }

            Table table = this.connection.OpenTable(tablename);
            this.cachedTable = table;
            return table;
        }

        /// <summary>
        /// Close the cached table.
        /// </summary>
        private void CloseCachedTable()
        {
            if (null != this.cachedTable)
            {
                this.Tracer.TraceInfo("closing cached table '{0}'", this.cachedTable.TableName);
                this.cachedTable.Dispose();
                this.cachedTable = null;
            }
        }

        /// <summary>
        /// Close the existing connection.
        /// </summary>
        private void CloseConnection()
        {
            this.CloseCachedTable();

            // Retrieve the variable directly to avoid the null check
            // in the property.
            if (null != this.connection)
            {
                this.Tracer.TraceInfo("closing connection to '{0}'", this.Connection.Database);
                this.Connection.Dispose();
                this.Connection = null;
            }
        }

        /// <summary>
        /// Called when an invalid command has been used (e.g. SELECT without a database attached).
        /// Logs the error and throws an exception.
        /// </summary>
        /// <param name="error">Description of the error.</param>
        private void SqlExecutionError(string error)
        {
            this.Tracer.TraceError(error);
            throw new EsentSqlExecutionException(error);
        }

        /// <summary>
        /// Stores information about a savepoint.
        /// </summary>
        private struct Savepoint
        {
            /// <summary>
            /// Gets or sets the name of the savepoint.
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the Transaction object for
            /// the savepoint.
            /// </summary>
            public Transaction Transaction { get; set; }
        }
    }
}