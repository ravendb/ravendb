using Raven.Database.Indexing;

namespace Raven.Client.Indexes
{
	
#if !NET_3_5
	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
	[System.ComponentModel.Composition.InheritedExport]
#endif
	public abstract class AbstractIndexCreationTask
	{
		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public abstract IndexDefinition CreateIndexDefinition();

		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
		public virtual string IndexName { get { return GetType().Name.Replace("_", "/"); } }

		/// <summary>
		/// Gets or sets the document store.
		/// </summary>
		/// <value>The document store.</value>
		public IDocumentStore DocumentStore { get; private set; }

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		/// <param name="documentStore">The document store.</param>
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