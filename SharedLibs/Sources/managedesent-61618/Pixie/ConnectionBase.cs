//-----------------------------------------------------------------------
// <copyright file="ConnectionBase.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A connection to a database.
    /// </summary>
    /// <remarks>
    /// This contains the common code needed by ReadOnlyConnection and
    /// ReadWriteConnection.
    /// </remarks>
    internal abstract class ConnectionBase : Connection
    {
        /// <summary>
        /// All tables currently opened on the database. Used to close
        /// them when the connection is closed.
        /// </summary>
        private readonly List<Table> openedTables;

        /// <summary>
        /// The ESE session for this connection.
        /// </summary>
        private readonly Session session;

        /// <summary>
        /// The ESE database for this connection.
        /// </summary>
        private JET_DBID dbid;

        /// <summary>
        /// Set to true when the connection is disposed.
        /// </summary>
        private bool isDisposed;

        /// <summary>
        /// The outer transaction for this Connection. If this member is
        /// null then there is no active transaction (we are at level 0).
        /// </summary>
        private EsentTransaction level0Transaction;

        /// <summary>
        /// Initializes a new instance of the ConnectionBase class.
        /// </summary>
        /// <param name="instance">The instance to use for the connection.</param>
        /// <param name="name">The name of the connection.</param>
        /// <param name="database">The database to be connected do.</param>
        /// <param name="grbit">The option to open the database with.</param>
        protected ConnectionBase(Instance instance, string name, string database, OpenDatabaseGrbit grbit)
        {
            this.Tracer = new Tracer("Connection", "Esent Connection", String.Format("Connection {0}", name));

            this.Name = name;
            this.Database = database;
            this.openedTables = new List<Table>();

            this.session = new Session(instance);
            Api.JetAttachDatabase(this.session, database, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(this.session, this.Database, String.Empty, out this.dbid, grbit);

            this.Tracer.TraceInfo("created. (database '{0}')", this.Database);
        }

        /// <summary>
        /// Event that is called when the Connection is closed.
        /// </summary>
        public event Action<ConnectionBase> Disposed;

        /// <summary>
        /// Gets the name of the ESE instance.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the full path of the database.
        /// </summary>
        public string Database { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the Connection is inside a transaction.
        /// </summary>
        public bool InTransaction
        {
            get
            {
                return null != this.level0Transaction;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the connection is read-only.
        /// </summary>
        /// <remarks>
        /// Deferred to ReadOnlyConnection and ReadWriteConnection.
        /// </remarks>
        public abstract bool IsReadOnly { get; }

        /// <summary>
        /// Gets the underlying ESE session for this connection
        /// </summary>
        public Session Session
        {
            get
            {
                this.CheckNotDisposed();
                return this.session;
            }
        }

        /// <summary>
        /// Gets the underlying ESE database for this connection
        /// </summary>
        public JET_DBID Dbid
        {
            get
            {
                this.CheckNotDisposed();
                return this.dbid;
            }
        }

        /// <summary>
        /// Gets the tracing object for this Connection.
        /// </summary>
        public Tracer Tracer { get; private set; }

        /// <summary>
        /// Called when the object is disposed.
        /// </summary>
        public void Dispose()
        {
            if (!this.isDisposed)
            {
                this.CloseAllOpenTables();

                Api.JetCloseDatabase(this.session, this.dbid, CloseDatabaseGrbit.None);
                this.session.End();

                this.dbid = JET_DBID.Nil;

                this.Tracer.TraceInfo("closed (database '{0}')", this.Database);

                Action<ConnectionBase> disposedEvent = this.Disposed;
                if (null != disposedEvent)
                {
                    disposedEvent(this);
                }

                this.isDisposed = true;
            }

            Debug.Assert(this.isDisposed, "Close called but isDisposed is false");
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Begin a new transaction.
        /// </summary>
        /// <returns>A new transaction object.</returns>
        public Transaction BeginTransaction()
        {
            string transactionName = String.Format("{0}-{1}", this.Name, DateTime.Now);
            var transaction = new EsentTransaction(this.session, transactionName, this.GetNewestTransaction());
            if (null == this.level0Transaction)
            {
                this.OnBeginLevel0Transaction(transaction);
            }

            return transaction;
        }

        /// <summary>
        /// Perform an operation inside of a transaction. The transaction is durably
        /// committed if the operation completes normally, otherwise it rolls back.
        /// </summary>
        /// <param name="block">The operation to perform.</param>
        /// <returns>The Connection that performed the transaction.</returns>
        public Connection UsingTransaction(Action block)
        {
            return this.UsingTransaction(block, CommitOptions.Durable);
        }

        /// <summary>
        /// Perform an operation inside of a transaction. The transaction is committed
        /// lazy if the operation completes normally, otherwise it rolls back.
        /// </summary>
        /// <param name="block">The operation to perform.</param>
        /// <returns>The Connection that performed the transaction.</returns>
        public Connection UsingLazyTransaction(Action block)
        {
            return this.UsingTransaction(block, CommitOptions.Fast);
        }

        /// <summary>
        /// Create the given table.
        /// </summary>
        /// <param name="tablename">The table to create.</param>
        /// <remarks>
        /// Deferred to ReadOnlyConnection and ReadWriteConnection.
        /// </remarks>
        /// <returns>A new Table object for the table.</returns>
        public abstract Table CreateTable(string tablename);

        /// <summary>
        /// Open a table.
        /// </summary>
        /// <param name="tablename">The table to open.</param>
        /// <returns>A new Table object for the table.</returns>
        public Table OpenTable(string tablename)
        {
            this.CheckNotDisposed();

            TableBase table = this.OpenTableImpl(tablename);
            this.openedTables.Add(table);
            table.Disposed += this.OnTableClose;

            if (this.InTransaction)
            {
                EsentTransaction currentTransaction = this.level0Transaction.GetNewestTransaction();
                currentTransaction.RolledBack += table.Dispose;
            }

            this.Tracer.TraceVerbose("opened table '{0}'", tablename);
            return table;
        }

        /// <summary>
        /// Open a new Cursor.
        /// </summary>
        /// <param name="tablename">The name of the table to open the cursor on.</param>
        /// <returns>A new cursor opened on the table.</returns>
        public Cursor OpenCursor(string tablename)
        {
            var cursor = new Cursor(this.session, this.dbid, tablename);

            if (this.InTransaction)
            {
                EsentTransaction currentTransaction = this.level0Transaction.GetNewestTransaction();
                currentTransaction.RolledBack += cursor.Dispose;
            }

            this.Tracer.TraceVerbose("opened cursor on table '{0}'", tablename);
            return cursor;
        }

        /// <summary>
        /// Create a new table object for the specified table. This is specialized
        /// by the subclasses of Connection.
        /// </summary>
        /// <remarks>
        /// Deferred to ReadOnlyConnection and ReadWriteConnection.
        /// </remarks>
        /// <param name="tablename">The table to create the table object for.</param>
        /// <returns>A new table object.</returns>
        protected abstract TableBase OpenTableImpl(string tablename);

        /// <summary>
        /// Make sure the connection has not been disposed.
        /// </summary>
        protected void CheckNotDisposed()
        {
            if (this.isDisposed)
            {
                this.Tracer.TraceError("InvalidOperationException: Connection has been closed");
                throw new InvalidOperationException(String.Format("Connection {0} has been closed", this.Name));
            }
        }

        /// <summary>
        /// Get the newest transaction in the Connection.
        /// </summary>
        /// <returns>The newest transaction, or null if the Connection is not in a transaction.</returns>
        private EsentTransaction GetNewestTransaction()
        {
            return (null == this.level0Transaction) ? null : this.level0Transaction.GetNewestTransaction();
        }

        /// <summary>
        /// Called when a new level 0 transaction starts.
        /// </summary>
        /// <param name="transaction">The transaction that is starting.</param>
        private void OnBeginLevel0Transaction(EsentTransaction transaction)
        {
            Debug.Assert(null == this.level0Transaction, "Already in a transaction");
            transaction.Committed += this.CloseLevel0Transaction;
            transaction.RolledBack += this.CloseLevel0Transaction;
            this.level0Transaction = transaction;
        }

        /// <summary>
        /// Called when the level 0 transaction commits or rolls back. Clears the
        /// level0Transaction member.
        /// </summary>
        private void CloseLevel0Transaction()
        {
            this.level0Transaction = null;
        }

        /// <summary>
        /// Perform an operation inside of a transaction. The transaction is committed
        /// if the operation completes normally.
        /// </summary>
        /// <param name="block">The operation to perform.</param>
        /// <param name="options">Commit option for the transaction.</param>
        /// <returns>The Connection that performed the transaction.</returns>
        private Connection UsingTransaction(Action block, CommitOptions options)
        {
            using (var trx = this.BeginTransaction())
            {
                block();
                trx.Commit(options);
            }

            return this;
        }

        /// <summary>
        /// Called when a table is being closed. This removes the table from
        /// the list of open tables.
        /// </summary>
        /// <param name="table">The table being closed.</param>
        private void OnTableClose(TableBase table)
        {
            this.CheckNotDisposed();

            this.openedTables.Remove(table);
            this.Tracer.TraceVerbose("table '{0}' was closed", table.TableName);
        }

        /// <summary>
        /// Close all opened tables.
        /// </summary>
        private void CloseAllOpenTables()
        {
            // closing tables will modify this collection. copy it to an array before closing
            // the tables
            Table[] tablesToClose = this.openedTables.ToArray();
            foreach (Table table in tablesToClose)
            {
                table.Dispose();
            }

            Debug.Assert(this.openedTables.Count == 0, "Closed all tables, expected this.openedTables to be empty");
        }
    }
}
