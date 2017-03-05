//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            if (id == null)
                return default(T);
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();

            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public Dictionary<string, T> Load<T>(IEnumerable<string> ids)
        {
            return LoadInternal<T>(ids.ToArray());
        }


        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var result = LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var result = LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var result = LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);
            loadOeration.WithIncludes(includes);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string transformer, Dictionary<string, object> transformerParameters = null)
        {
            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));
            if (ids.Length == 0)
                return new Dictionary<string, T>();

            var operation = new LoadTransformerOperation(this);
            operation.ByIds(ids);
            operation.WithTransformer(transformer, transformerParameters);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                operation.SetResult(command.Result);
            }

            return operation.GetTransformedDocuments<T>(command?.Result);
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters = null)
        {
            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));
            if (ids.Length == 0)
                return new Dictionary<string, T>();

            var loadTransformerOeration = new LoadTransformerOperation(this);
            loadTransformerOeration.ByIds(ids);
            loadTransformerOeration.WithTransformer(transformer, transformerParameters);
            loadTransformerOeration.WithIncludes(includes);

            var command = loadTransformerOeration.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadTransformerOeration.SetResult(command.Result);
            }

            return loadTransformerOeration.GetTransformedDocuments<T>(command?.Result);
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            string startAfter = null)
        {
            IncrementRequestCount();

            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            loadStartingWithOperation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, startAfter: startAfter);

            var command = loadStartingWithOperation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadStartingWithOperation.SetResult(command.Result);
            }

            return loadStartingWithOperation.GetDocuments<T>();
        }

        public Dictionary<string, TResult> LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            IncrementRequestCount();
            var transformer = new TTransformer().TransformerName;

            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            loadStartingWithOperation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, configure, startAfter);
            loadStartingWithOperation.WithTransformer(transformer, configuration.TransformerParameters);


            var command = loadStartingWithOperation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
            }

            return loadStartingWithOperation.GetTransformedDocuments<TResult>(command?.Result);
        }
    }
}