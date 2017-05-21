using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Util;

namespace Raven.Client.Documents.Transformers
{
    /// <summary>
    /// Base class for creating transformers
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    public abstract class AbstractTransformerCreationTask : AbstractCommonApiForIndexesAndTransformers
    {
        /// <summary>
        /// Generates transformer name from type name replacing all _ with /
        /// <para>e.g.</para>
        /// <para>if our type is <code>'Orders_Totals'</code> then index name would be <code>'Orders/Totals'</code></para>
        /// </summary>
        public virtual string TransformerName => GetType().Name.Replace("_", "/");

        /// <summary>
        /// Returns value of a transformer parameter for specified key
        /// </summary>
        protected TransformerParameter Parameter(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Returns value of a transformer parameter for specified key or specified default value if there is no parameter under given key
        /// </summary>
        protected TransformerParameter ParameterOrDefault(string key, object defaultVal)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformer with given name on a specified items
        /// </summary>
        protected IEnumerable<object> TransformWith<T>(string transformer, IEnumerable<T> items)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformer with given name on a specified items
        /// </summary>
        protected IEnumerable<TResult> TransformWith<T, TResult>(string transformer, IEnumerable<T> items)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformer with given name on a specified item
        /// </summary>
        protected IEnumerable<object> TransformWith<T>(string transformer, T item)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformer with given name on a specified item
        /// </summary>
        protected IEnumerable<TResult> TransformWith<T, TResult>(string transformer, T item)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformers with given names on a specified items
        /// </summary>
        protected IEnumerable<object> TransformWith<T>(IEnumerable<string> transformers, IEnumerable<T> items)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformers with given names on a specified items
        /// </summary>
        protected IEnumerable<TResult> TransformWith<T, TResult>(IEnumerable<string> transformers, IEnumerable<T> items)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformers with given names on a specified item
        /// </summary>
        protected IEnumerable<object> TransformWith<T>(IEnumerable<string> transformers, T item)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Applies transformers with given names on a specified item
        /// </summary>
        protected IEnumerable<TResult> TransformWith<T, TResult>(IEnumerable<string> transformers, T item)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Gets or sets the conventions that should be used when index definition is created.
        /// </summary>
        public DocumentConventions Conventions { get; set; }

        /// <summary>
        /// Creates the transformer definition.
        /// </summary>
        public abstract TransformerDefinition CreateTransformerDefinition(bool prettify = true);

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
        public virtual void Execute(IDocumentStore store, DocumentConventions conventions)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(store, conventions));
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public virtual Task ExecuteAsync(IDocumentStore store, DocumentConventions conventions, CancellationToken token = default(CancellationToken))
        {
            Conventions = conventions;
            var prettify = conventions.PrettifyGeneratedLinqExpressions;
            var transformerDefinition = CreateTransformerDefinition(prettify);

            if (transformerDefinition.Name == null)
                transformerDefinition.Name = TransformerName;

            return store.Admin.SendAsync(new PutTransformerOperation(transformerDefinition), token);
        }
    }

    public class AbstractTransformerCreationTask<TFrom> : AbstractTransformerCreationTask
    {
        protected Expression<Func<IEnumerable<TFrom>, IEnumerable>> TransformResults { get; set; }

        public object Include(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T Include<T>(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public IEnumerable<T> Include<T>(IEnumerable<string> key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public object Include(IEnumerable<string> key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public override TransformerDefinition CreateTransformerDefinition(bool prettify = true)
        {
            var transformerDefinition = new TransformerDefinition
            {
                Name = TransformerName,
                TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TFrom, object>(
                    TransformResults, Conventions, "results", translateIdentityProperty: false),
            };

            if (prettify)
            {
                transformerDefinition.TransformResults = IndexPrettyPrinter.TryFormat(transformerDefinition.TransformResults);
            }

            return transformerDefinition;
        }
    }
}
