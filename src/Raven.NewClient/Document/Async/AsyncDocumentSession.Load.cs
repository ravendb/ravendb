using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document.Async
{
    public partial class AsyncDocumentSession
    {
        /// <summary>
        /// Begins the async load operation, with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T> LoadAsync<T>(ValueType id, CancellationToken token = default(CancellationToken))
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return LoadAsync<T>(documentKey, token);
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(CancellationToken token = default(CancellationToken), params ValueType[] ids)
        {
            var documentKeys =
                ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<T>(documentKeys, token);
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids)
        {
            return LoadAsync<T>(ids, new CancellationToken());
        }

        /// <summary>
        /// Begins the async load operation, with the specified ids after applying
        /// conventions on the provided ids to get the real document ids.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// LoadAsync{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// LoadAsync{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public Task<T[]> LoadAsync<T>(IEnumerable<ValueType> ids, CancellationToken token = default(CancellationToken))
        {
            var documentKeys =
                ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LoadAsync<T>(documentKeys, token);
        }

        public async Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var result = await LoadAsync<TTransformer, TResult>(new[] { id }.AsEnumerable(), configure, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids,
            Action<ILoadConfiguration> configure = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer();
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer.TransformerName, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result;
        }

        public async Task<TResult> LoadAsync<TResult>(string id, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            return await LoadUsingTransformerInternalAsync<TResult>(ids.ToArray(), null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
        }

        public async Task<TResult> LoadAsync<TResult>(string id, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new RavenLoadConfiguration();
            configure?.Invoke(configuration);

            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;

            var result = await LoadUsingTransformerInternalAsync<TResult>(new[] { id }, null, transformer, configuration.TransformerParameters, token).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var configuration = new RavenLoadConfiguration();
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

        public async Task<T[]> LoadAsync<T>(IEnumerable<string> ids,
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

        public async Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes,
            CancellationToken token = new CancellationToken())
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);
            loadOeration.WithIncludes(includes?.Select(x => x.Key).ToArray());

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public async Task<T[]> LoadUsingTransformerInternalAsync<T>(string[] ids, KeyValuePair<string, Type>[] includes,
            string transformer, Dictionary<string, object> transformerParameters = null,
            CancellationToken token = default(CancellationToken))
        {
            if (transformer == null)
                throw new ArgumentNullException("transformer");
            if (ids.Length == 0)
                return new T[0];

            var loadTransformerOeration = new LoadTransformerOperation(this);
            loadTransformerOeration.ByIds(ids);
            loadTransformerOeration.WithTransformer(transformer, transformerParameters);
            loadTransformerOeration.WithIncludes(includes?.Select(x => x.Key).ToArray());

            var command = loadTransformerOeration.CreateRequest();
            if (command != null)
            {
                await RequestExecuter.ExecuteAsync(command, Context, token);
                loadTransformerOeration.SetResult(command.Result);
            }

            return loadTransformerOeration.GetTransformedDocuments<T>(command?.Result);
        }

        public Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix,
            string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null,
            Action<ILoadConfiguration> configure = null,
            string skipAfter = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null,
            string skipAfter = null, CancellationToken token = default(CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}