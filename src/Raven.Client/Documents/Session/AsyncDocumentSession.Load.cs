using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {

        public async Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var result = await LoadAsync<TTransformer, TResult>(new[] { id }.AsEnumerable(), configure, token).ConfigureAwait(false);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public async Task<Dictionary<string, TResult>> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids,
            Action<ILoadConfiguration> configure = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer();
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer.TransformerName, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result;
        }

        public async Task<TResult> LoadAsync<TResult>(string id, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public async Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            return await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
        }

        public async Task<TResult> LoadAsync<TResult>(string id, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            if (result.Count == 0)
                return default(TResult);

            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public async Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            return await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="token">The canecllation token.</param>
        /// <returns></returns>
        public async Task<T> LoadAsync<T>(string id, CancellationToken token = default(CancellationToken))
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        public async Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids,
            CancellationToken token = default(CancellationToken))
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public async Task<Dictionary<string, T>> LoadAsyncInternal<T>(string[] ids, string[] includes,
            CancellationToken token = new CancellationToken())
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);
            loadOeration.WithIncludes(includes?.ToArray());

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public async Task<Dictionary<string, T>> LoadUsingTransformerInternalAsync<T>(string[] ids, string[] includes, string transformer,
            Dictionary<string, object> transformerParameters = null, CancellationToken token = new CancellationToken())
        {
            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));
            if (ids.Length == 0)
                return new Dictionary<string, T>();

            var loadTransformerOeration = new LoadTransformerOperation(this);
            loadTransformerOeration.ByIds(ids);
            loadTransformerOeration.WithTransformer(transformer, transformerParameters);
            loadTransformerOeration.WithIncludes(includes?.ToArray());

            var command = loadTransformerOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadTransformerOeration.SetResult(command.Result);
            }

            return loadTransformerOeration.GetTransformedDocuments<T>(command?.Result);
        }

        public async Task<Dictionary<string, TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix,
            string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, PagingInformation pagingInformation = null,
            Action<ILoadConfiguration> configure = null,
            string startAfter = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            IncrementRequestCount();
            var transformer = new TTransformer().TransformerName;

            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            loadStartingWithOperation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, pagingInformation, configure, startAfter);
            loadStartingWithOperation.WithTransformer(transformer, configuration.TransformerParameters);


            var command = loadStartingWithOperation.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
            }

            return loadStartingWithOperation.GetTransformedDocuments<TResult>(command?.Result);
        }

        public async Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, PagingInformation pagingInformation = null,
            string startAfter = null, CancellationToken token = default(CancellationToken))
        {
            IncrementRequestCount();

            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            loadStartingWithOperation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, pagingInformation, startAfter: startAfter);

            var command = loadStartingWithOperation.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadStartingWithOperation.SetResult(command.Result);
            }

            return loadStartingWithOperation.GetDocuments<T>();
        }
    }
}