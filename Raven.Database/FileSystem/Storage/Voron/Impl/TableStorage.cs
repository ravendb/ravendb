// -----------------------------------------------------------------------
//  <copyright file="TableStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Util.Streams;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

using Voron;
using Voron.Debugging;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;

namespace Raven.Database.FileSystem.Storage.Voron.Impl
{
    internal class TableStorage : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly IBufferPool bufferPool;

        private readonly StorageEnvironment env;

        public TableStorage(StorageEnvironmentOptions options, IBufferPool bufferPool)
        {
            if (options == null)
                throw new ArgumentNullException("options");

            _options = options;
            this.bufferPool = bufferPool;

            env = new StorageEnvironment(_options);

            Initialize();
        }

        internal Dictionary<string, object> GenerateReportOnStorage()
        {
            var reportData = new Dictionary<string, object>
	        {
	            {"NumberOfAllocatedPages", _options.DataPager.NumberOfAllocatedPages},
                {"UsedPages", env.State.NextPageNumber-1},
	            {"MaxNodeSize", AbstractPager.NodeMaxSize},
	            {"PageMinSpace", _options.DataPager.PageMinSpace},
	            {"PageMaxSpace", AbstractPager.PageMaxSpace},
	            {"PageSize", AbstractPager.PageSize},
                {"Files", GetEntriesCount(Files)},
	        };

            return reportData;
        }

        public SnapshotReader CreateSnapshot()
        {
            return env.CreateSnapshot();
        }

        public Table Files { get; private set; }

        public Table Signatures { get; private set; }

        public Table Config { get; private set; }

        public Table Usage { get; private set; }

        public Table Pages { get; private set; }

        public Table Details { get; private set; }

        public StorageEnvironment Environment
        {
            get
            {
                return env;
            }
        }

        public void Write(WriteBatch writeBatch)
        {
            try
            {
                env.Writer.Write(writeBatch);
            }
            catch (AggregateException ae)
            {
                if (ae.InnerException is OperationCanceledException == false) // this can happen during storage disposal
                    throw;
            }
        }

        public long GetEntriesCount(TableBase table)
        {
            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                return tx.State.GetTree(tx, table.TableName).State.EntriesCount;
            }
        }

        public void RenderAndShow(TableBase table, int showEntries = 25)
        {
            if (Debugger.IsAttached == false)
                return;

            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                RenderAndShow(tx, table, showEntries);
            }
        }

        public void RenderAndShow(Transaction tx, TableBase table, int showEntries = 25)
        {
            if (Debugger.IsAttached == false)
                return;

            var tree = tx.State.GetTree(tx, table.TableName);

            var path = Path.Combine(System.Environment.CurrentDirectory, "test-tree.dot");
            var rootPageNumber = tree.State.RootPageNumber;
            TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(rootPageNumber), showEntries);

            var output = Path.Combine(System.Environment.CurrentDirectory, "output.svg");
            var p = Process.Start(@"c:\Program Files (x86)\Graphviz2.32\bin\dot.exe", "-Tsvg  " + path + " -o " + output);
            p.WaitForExit();
            Process.Start(output);
        }

        public void Dispose()
        {
            if (env != null)
                env.Dispose();
        }

        private void Initialize()
        {
            Files = new Table(Tables.Files.TableName, bufferPool);
            Signatures = new Table(Tables.Signatures.TableName, bufferPool, Tables.Signatures.Indices.ByName);
            Config = new Table(Tables.Config.TableName, bufferPool);
            Usage = new Table(Tables.Usage.TableName, bufferPool);
            Pages = new Table(Tables.Pages.TableName, bufferPool, Tables.Pages.Indices.Data, Tables.Pages.Indices.ByKey);
            Details = new Table(Tables.Details.TableName, bufferPool);
        }

        public void SetDatabaseIdAndSchemaVersion(Guid id, string schemaVersion)
        {
            Id = id;
            SchemaVersion = schemaVersion;
        }

        public string SchemaVersion { get; private set; }

        public Guid Id { get; private set; }

    }
}