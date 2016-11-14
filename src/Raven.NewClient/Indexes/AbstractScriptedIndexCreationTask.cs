// -----------------------------------------------------------------------
//  <copyright file="AbstractScriptedIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Indexes
{
    public abstract class AbstractScriptedIndexCreationTask : AbstractIndexCreationTask
    {
        private readonly ScriptedIndexResults scripts;

        protected AbstractScriptedIndexCreationTask()
        {
            scripts = new ScriptedIndexResults();
        }

        public string IndexScript
        {
            get { return scripts.IndexScript; }
            set { scripts.IndexScript = value; }
        }

        public string DeleteScript
        {
            get { return scripts.DeleteScript; }
            set { scripts.DeleteScript = value; }
        }

        public bool RetryOnConcurrencyExceptions
        {
            get { return scripts.RetryOnConcurrencyExceptions; }
            set { scripts.RetryOnConcurrencyExceptions = value; }
        }

        public override void AfterExecute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
        {
            base.AfterExecute(databaseCommands, documentConvention);
            AfterExecute(databaseCommands, IndexName, scripts);
        }

        public override async Task AfterExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention, CancellationToken token = default(CancellationToken))
        {
            await base.AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token).ConfigureAwait(false);
            await AfterExecuteAsync(asyncDatabaseCommands, IndexName, scripts, token).ConfigureAwait(false);
        }

        internal static void AfterExecute(IDatabaseCommands databaseCommands, string indexName, ScriptedIndexResults scripts)
        {
            var documentId = GetScriptedIndexResultsDocumentId(indexName);
            scripts.Id = documentId;

            var oldDocument = databaseCommands.Get(documentId);
            var newDocument = RavenJObject.FromObject(scripts);
            if (oldDocument != null && RavenJToken.DeepEquals(oldDocument.DataAsJson, newDocument))
                return;

            databaseCommands.Put(documentId, null, newDocument, null);
            databaseCommands.ResetIndex(indexName);
        }

        internal static async Task AfterExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, string indexName, ScriptedIndexResults scripts, CancellationToken token)
        {
            var documentId = GetScriptedIndexResultsDocumentId(indexName);
            scripts.Id = documentId;

            var oldDocument = await asyncDatabaseCommands.GetAsync(documentId, token: token).ConfigureAwait(false);
            var newDocument = RavenJObject.FromObject(scripts);
            if (oldDocument != null && RavenJToken.DeepEquals(oldDocument.DataAsJson, newDocument))
                return;

            await asyncDatabaseCommands.PutAsync(documentId, null, newDocument, null, token).ConfigureAwait(false);
            await asyncDatabaseCommands.ResetIndexAsync(indexName, token).ConfigureAwait(false);
        }

        private static string GetScriptedIndexResultsDocumentId(string indexName)
        {
            return ScriptedIndexResults.IdPrefix + indexName;
        }
    }

    public abstract class AbstractScriptedIndexCreationTask<TDocument, TReduceResult> : AbstractIndexCreationTask<TDocument, TReduceResult>
    {
        private readonly ScriptedIndexResults scripts;

        protected AbstractScriptedIndexCreationTask()
        {
            scripts = new ScriptedIndexResults();
        }

        public string IndexScript
        {
            get { return scripts.IndexScript; }
            set { scripts.IndexScript = value; }
        }

        public string DeleteScript
        {
            get { return scripts.DeleteScript; }
            set { scripts.DeleteScript = value; }
        }

        public bool RetryOnConcurrencyExceptions
        {
            get { return scripts.RetryOnConcurrencyExceptions; }
            set { scripts.RetryOnConcurrencyExceptions = value; }
        }

        public override void AfterExecute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
        {
            base.AfterExecute(databaseCommands, documentConvention);
            AbstractScriptedIndexCreationTask.AfterExecute(databaseCommands, IndexName, scripts);
        }

        public override async Task AfterExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention, CancellationToken token = default(CancellationToken))
        {
            await base.AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token).ConfigureAwait(false);
            await AbstractScriptedIndexCreationTask.AfterExecuteAsync(asyncDatabaseCommands, IndexName, scripts, token).ConfigureAwait(false);
        }
    }

    public abstract class AbstractScriptedIndexCreationTask<TDocument> : AbstractScriptedIndexCreationTask<TDocument, TDocument>
    {
    }
}
