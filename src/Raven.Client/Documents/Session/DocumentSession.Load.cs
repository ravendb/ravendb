//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;
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
            var loadOperation = new LoadOperation(this);
            loadOperation.ById(id);

            var command = loadOperation.CreateRequest();

            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public Dictionary<string, T> Load<T>(IEnumerable<string> ids)
        {
            var loadOperation = new LoadOperation(this);
            LoadInternal(ids.ToArray(), loadOperation);
            return loadOperation.GetDocuments<T>();
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(new[] { id }, new TTransformer().TransformerName, operation ,null, configure);
            if (command == null)
                return default(TResult);
            var result = operation.GetTransformedDocuments<TResult>(command.Result);
            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(ids.ToArray(), new TTransformer().TransformerName, operation, null, configure);
            return operation.GetTransformedDocuments<TResult>(command.Result);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure)
        {
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(new[] { id }, transformer, operation, null, configure);
            if (command == null)
                return default(TResult);
            var result = operation.GetTransformedDocuments<TResult>(command.Result);
            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(ids.ToArray(), transformer, operation, null, configure);
            return operation.GetTransformedDocuments<TResult>(command.Result);
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(new[] { id }, transformer, operation, null, configure);
            if (command == null)
                return default(TResult);
            var result = operation.GetTransformedDocuments<TResult>(command.Result);
            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public Dictionary<string, TResult> Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var operation = new LoadTransformerOperation(this);
            var command = LoadInternal(ids.ToArray(), transformer, operation, null, configure);
            return operation.GetTransformedDocuments<TResult>(command.Result);
        }

        private void LoadInternal(string[] ids, LoadOperation operation, Stream stream = null)
        {
            operation.ByIds(ids);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                if(stream!=null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes)
        {
            var loadOperation = new LoadOperation(this);
            loadOperation.ByIds(ids);
            loadOperation.WithIncludes(includes);

            var command = loadOperation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocuments<T>();
        }

        private GetDocumentCommand LoadInternal(string[] ids, string transformer, LoadTransformerOperation operation, Stream stream = null ,Action<ILoadConfiguration> configure = null)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));
            if (ids.Length == 0)
                return null;

            operation.ByIds(ids);
            operation.WithTransformer(transformer, configuration.TransformerParameters);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);

                if(stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else            
                    operation.SetResult(command.Result);
            }

            return command;
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters = null)
        {
            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));
            if (ids.Length == 0)
                return new Dictionary<string, T>();

            var loadTransformerOperation = new LoadTransformerOperation(this);
            loadTransformerOperation.ByIds(ids);
            loadTransformerOperation.WithTransformer(transformer, transformerParameters);
            loadTransformerOperation.WithIncludes(includes);

            var command = loadTransformerOperation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);
                loadTransformerOperation.SetResult(command.Result);
            }

            return loadTransformerOperation.GetTransformedDocuments<T>(command?.Result);
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            string startAfter = null)
        {
            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            LoadStartingWithInternal(keyPrefix, loadStartingWithOperation, null, matches, start, pageSize, exclude, null, startAfter);
            return loadStartingWithOperation.GetDocuments<T>();
        }

        public Dictionary<string, TResult> LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            var command = LoadStartingWithInternal(keyPrefix,loadStartingWithOperation ,null, matches, start, pageSize, exclude,
                configure, startAfter, new TTransformer().TransformerName);

            return loadStartingWithOperation.GetTransformedDocuments<TResult>(command?.Result);
        }

        public void LoadStartingWithIntoStream(string keyPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            string startAfter = null)
        {
            LoadStartingWithInternal(keyPrefix, new LoadStartingWithOperation(this), output, matches, start, pageSize, exclude, null, startAfter);
        }

        public void LoadStartingWithIntoStream<TTransformer>(string keyPrefix, Stream output, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            LoadStartingWithInternal(keyPrefix, new LoadStartingWithOperation(this), output, matches, start, pageSize, exclude, configure, startAfter, new TTransformer().TransformerName);
        }

        private GetDocumentCommand LoadStartingWithInternal(string keyPrefix, LoadStartingWithOperation operation, Stream stream = null, string matches = null,
            int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null, string transformer = null)
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            operation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, configure, startAfter);

            if (transformer != null)
                operation.WithTransformer(transformer, configuration.TransformerParameters);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context);

                if (stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }

            return command;
        }

        public void LoadIntoStream(IEnumerable<string> ids, Stream output)
        {
            LoadInternal(ids.ToArray(), new LoadOperation(this), output);
        }

        public void LoadIntoStream<TTransformer>(IEnumerable<string> ids, Stream output, Action<ILoadConfiguration> configure = null)
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            LoadInternal(ids.ToArray(), new TTransformer().TransformerName, new LoadTransformerOperation(this), output, configure);
        }

        public void LoadIntoStream(IEnumerable<string> ids, string transformer, Stream output,
            Action<ILoadConfiguration> configure = null)
        {
            LoadInternal(ids.ToArray(), transformer, new LoadTransformerOperation(this), output, configure);
        }

        public void LoadIntoStream(IEnumerable<string> ids, Type transformerType, Stream output,
            Action<ILoadConfiguration> configure = null)
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            LoadInternal(ids.ToArray(), transformer, new LoadTransformerOperation(this), output, configure);
        }
    }
}