//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Indexes;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();

            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public T[] Load<T>(IEnumerable<string> ids)
        {
            return LoadInternal<T>(ids.ToArray());
        }

        /// <summary>
        /// Loads the specified entity with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T Load<T>(ValueType id)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Load<T>(documentKey);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public T[] LoadInternal<T>(string[] ids)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);
            loadOeration.WithIncludes(includes.Select(x => x.Key).ToArray());

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
           RavenPagingInformation pagingInformation = null, string skipAfter = null)
        {
            throw new NotImplementedException();
        }

        public TResult[] LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, Action<ILoadConfiguration> configure = null,
            string skipAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }
    }
}