// -----------------------------------------------------------------------
//  <copyright file="AbstractScriptedIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

namespace Raven.Client.Indexes
{
    public abstract class AbstractScriptedIndexCreationTask
    {
        private readonly ScriptedIndexResults scripts;

        protected AbstractScriptedIndexCreationTask(string indexName)
        {
            scripts = new ScriptedIndexResults { Id = ScriptedIndexResults.IdPrefix + indexName };
        }
        protected string IndexScript
        {
            get { return scripts.IndexScript; }
            set { scripts.IndexScript = value; }
        }

        protected string DeleteScript
        {
            get { return scripts.DeleteScript; }
            set { scripts.DeleteScript = value; }
        }

        public bool RetryOnConcurrencyExceptions
        {
            get { return scripts.RetryOnConcurrencyExceptions; }
            set { scripts.RetryOnConcurrencyExceptions = value; }
        }

        public void Execute(IDocumentStore store)
        {
            store.DatabaseCommands.Put(scripts.Id, null, RavenJObject.FromObject(scripts), null);
        }

        public void Execute(IDatabaseCommands databaseCommands)
        {
            databaseCommands.Put(scripts.Id, null, RavenJObject.FromObject(scripts), null);
        }

        public Task ExecuteAsync(IDocumentStore store)
        {
            return store.AsyncDatabaseCommands.PutAsync(scripts.Id, null, RavenJObject.FromObject(scripts), null);
        }

        public Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            return asyncDatabaseCommands.PutAsync(scripts.Id, null, RavenJObject.FromObject(scripts), null);
        }
    }
    public abstract class AbstractScriptedIndexCreationTask<TIndex> : AbstractScriptedIndexCreationTask where TIndex : AbstractIndexCreationTask
    {
        protected AbstractScriptedIndexCreationTask() : base(typeof(TIndex).Name.Replace("_", "/")) { }
    }
}
