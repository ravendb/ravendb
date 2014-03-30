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

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
    public class TableStorage : IDisposable
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
            CreateSchema();
        }

        internal Dictionary<string, object> GenerateReportOnStorage()
        {
            var reportData = new Dictionary<string, object>
	        {
	            {"MaxNodeSize", _options.DataPager.MaxNodeSize},
	            {"NumberOfAllocatedPages", _options.DataPager.NumberOfAllocatedPages},
	           // {"PageMaxSpace", _options.DataPager.PageMaxSpace},
	            {"PageMinSpace", _options.DataPager.PageMinSpace},
	           // {"PageSize", _options.DataPager.PageSize},
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

        //create all relevant storage trees in one place
        private void CreateSchema()
        {
            using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                CreateFilesSchema(tx);
                CreatSignaturesSchema(tx);
                CreateConfigSchema(tx);
                CreateUsageSchema(tx);
                CreatePagesSchema(tx);
                CreateDetailsSchema(tx);

                tx.Commit();
            }
        }

        private void CreatePagesSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Pages.TableName);
            env.CreateTree(tx, Pages.GetIndexKey(Tables.Pages.Indices.Data));
            env.CreateTree(tx, Pages.GetIndexKey(Tables.Pages.Indices.ByKey));
        }

        private void CreateUsageSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Usage.TableName);
            env.CreateTree(tx, Usage.GetIndexKey(Tables.Usage.Indices.ByFileName));
            env.CreateTree(tx, Usage.GetIndexKey(Tables.Usage.Indices.ByFileNameAndPosition));
        }

        private void CreateConfigSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Config.TableName);
        }

        private void CreatSignaturesSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Signatures.TableName);
            env.CreateTree(tx, Signatures.GetIndexKey(Tables.Signatures.Indices.Data));
            env.CreateTree(tx, Signatures.GetIndexKey(Tables.Signatures.Indices.ByName));
        }

        private void CreateFilesSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Files.TableName);
            env.CreateTree(tx, Files.GetIndexKey(Tables.Files.Indices.Count));
            env.CreateTree(tx, Files.GetIndexKey(Tables.Files.Indices.ByEtag));
        }

        private void CreateDetailsSchema(Transaction tx)
        {
            env.CreateTree(tx, Tables.Details.TableName);
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

    }
}