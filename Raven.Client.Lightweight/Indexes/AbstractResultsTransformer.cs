using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Base class for creating transformers
	/// </summary>
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
#if !MONO && !NETFX_CORE
	[System.ComponentModel.Composition.InheritedExport]
#endif
	public abstract class AbstractTransformerCreationTask : AbstractCommonApiForIndexesAndTransformers
	{
		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
		public virtual string TransformerName { get { return GetType().Name.Replace("_", "/"); } }

        protected RavenJToken Query(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

		/// <summary>
		/// Gets or sets the document store.
		/// </summary>
		/// <value>The document store.</value>
		public DocumentConvention Conventions { get; set; }

		/// <summary>
		/// Creates the Transformer definition.
		/// </summary>
		/// <returns></returns>
		public abstract TransformerDefinition CreateTransformerDefinition();

#if !SILVERLIGHT && !NETFX_CORE

		public void Execute(IDocumentStore store)
		{
			store.ExecuteTransformer(this);
		}

        public Task ExecuteAsync(IDocumentStore store)
        {
            return store.ExecuteTransformerAsync(this);
        }

		/// <summary>
		/// Executes the index creation against the specified document database using the specified conventions
		/// </summary>
		public virtual void Execute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var transformerDefinition = CreateTransformerDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new definition.
			databaseCommands.PutTransformer(TransformerName, transformerDefinition);

			UpdateIndexInReplication(databaseCommands, documentConvention, (commands, url) =>
				commands.PutTransformer(TransformerName, transformerDefinition));
		}
#endif

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public virtual Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var transformerDefinition = CreateTransformerDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new definition.
			return asyncDatabaseCommands.PutTransformerAsync(TransformerName, transformerDefinition)
				.ContinueWith(task => UpdateIndexInReplicationAsync(asyncDatabaseCommands, documentConvention, (client, url) =>
					client.DirectPutTransformerAsync(TransformerName, transformerDefinition, url)))
				.Unwrap();
		}
	}

    public class AbstractTransformerCreationTask<TFrom> : AbstractTransformerCreationTask
    {
		protected Expression<Func<IEnumerable<TFrom>, IEnumerable>> TransformResults { get; set; }

		public object Include(string key)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		public object Include(IEnumerable<string> key)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}
      
	    public override TransformerDefinition CreateTransformerDefinition()
	    {
		    return new TransformerDefinition
			{
				Name = TransformerName,
				TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TFrom, object>(
					TransformResults, Conventions, "results", translateIdentityProperty: false),
			};
	    }
    }
}
