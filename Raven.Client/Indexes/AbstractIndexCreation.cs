using System.ComponentModel.Composition;
using Raven.Database.Indexing;

namespace Raven.Client.Indexes
{
	[InheritedExport]
	public abstract class AbstractIndexCreationTask
	{
		public abstract IndexDefinition CreateIndexDefinition();

		public virtual string IndexName { get { return GetType().Name.Replace("_", "/"); } }

		public IDocumentStore DocumentStore { get; private set; }

		public virtual void Execute(IDocumentStore documentStore)
		{
			DocumentStore = documentStore;
			var indexDefinition = CreateIndexDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new defintion.
			documentStore.DatabaseCommands.PutIndex(IndexName, indexDefinition, true);
		}
	}
}