using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
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
            var operation = new LoadTransformerOperation(this);
            var command = await LoadUsingTransformerInternalAsync(ids.ToArray(), null, operation,
                new TTransformer().TransformerName, configure, token).ConfigureAwait(false);

            return operation.GetTransformedDocuments<TResult>(command?.Result);
        }

        public async Task<TResult> LoadAsync<TResult>(string id, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var operation = new LoadTransformerOperation(this);
            var command = await LoadUsingTransformerInternalAsync(new [] {id}, null, operation,
                transformer, configure, token).ConfigureAwait(false);

            if (command == null)
                return default(TResult);

            var result = operation.GetTransformedDocuments<TResult>(command.Result);
            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public async Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, string transformer,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var operation = new LoadTransformerOperation(this);
            var command = await LoadUsingTransformerInternalAsync(ids.ToArray(), null, operation,
                transformer, configure, token).ConfigureAwait(false);

            return operation.GetTransformedDocuments<TResult>(command?.Result);
        }

        public async Task<TResult> LoadAsync<TResult>(string id, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var operation = new LoadTransformerOperation(this);
            var command = await LoadUsingTransformerInternalAsync(new[] { id }, null, operation,
                transformer, configure, token).ConfigureAwait(false);

            if (command == null)
                return default(TResult);

            var result = operation.GetTransformedDocuments<TResult>(command.Result);
            Debug.Assert(result.Count == 1);

            return result[id];
        }

        public async Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType,
            Action<ILoadConfiguration> configure = null,
            CancellationToken token = new CancellationToken())
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            var operation = new LoadTransformerOperation(this);
            var command = await LoadUsingTransformerInternalAsync(ids.ToArray(), null, operation,
                transformer, configure, token).ConfigureAwait(false);

            return operation.GetTransformedDocuments<TResult>(command?.Result);
        }

        /// <summary>
        /// Begins the async load operation
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns></returns>
        public async Task<T> LoadAsync<T>(string id, CancellationToken token = default(CancellationToken))
        {
            var loadOperation = new LoadOperation(this);
            loadOperation.ById(id);

            var command = loadOperation.CreateRequest();
            if (command != null)
            {
                await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocument<T>();
        }

        public async Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids,
            CancellationToken token = default(CancellationToken))
        {
            var loadOperation = new LoadOperation(this);
            await LoadAsyncInternal(ids.ToArray(), null, loadOperation, token).ConfigureAwait(false);

            return loadOperation.GetDocuments<T>();
        }

        public async Task<Dictionary<string, T>> LoadAsyncInternal<T>(string[] ids, string[] includes,
            CancellationToken token = new CancellationToken())
        {
            var loadOperation = new LoadOperation(this);
            loadOperation.ByIds(ids);
            loadOperation.WithIncludes(includes?.ToArray());

            var command = loadOperation.CreateRequest();
            if (command != null)
            {
                await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocuments<T>();
        }

        private async Task<GetDocumentCommand> LoadUsingTransformerInternalAsync(string[] ids, Stream stream, LoadTransformerOperation operation, string transformer,
            Action<ILoadConfiguration> configure = null, CancellationToken token = new CancellationToken())
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
                await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);
                if(stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }

            return command;
        }

        public async Task<Dictionary<string, TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix,
            string matches = null, int start = 0,
            int pageSize = 25, string exclude = null,
            Action<ILoadConfiguration> configure = null,
            string startAfter = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            var operation = new LoadStartingWithOperation(this);
            var command = await LoadStartingWithInternal(keyPrefix, operation, null, matches, start,
                pageSize, exclude, configure, startAfter, new TTransformer().TransformerName, token).ConfigureAwait(false);

            return operation.GetTransformedDocuments<TResult>(command?.Result);
        }

        public async Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null,
            string startAfter = null, CancellationToken token = default(CancellationToken))
        {
            var operation = new LoadStartingWithOperation(this);
            await LoadStartingWithInternal(keyPrefix, operation, null, matches, start,
                pageSize, exclude, null, startAfter, null, token).ConfigureAwait(false);

            return operation.GetDocuments<T>();
        }

        public async Task LoadStartingWithIntoStreamAsync(string keyPrefix, Stream output, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, string startAfter = null, CancellationToken token = default(CancellationToken))
        {
            await LoadStartingWithInternal(keyPrefix, new LoadStartingWithOperation(this), output, matches, start,
                pageSize, exclude, null, startAfter, null, token).ConfigureAwait(false);
        }

        public async Task LoadStartingWithIntoStreamAsync<TTransformer>(string keyPrefix, Stream output, string matches = null,
            int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null, CancellationToken token = default(CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new()
        {
            await LoadStartingWithInternal(keyPrefix, new LoadStartingWithOperation(this), output, matches, start,
                pageSize, exclude, configure, startAfter, new TTransformer().TransformerName, token).ConfigureAwait(false);
        }

        private async Task<GetDocumentCommand> LoadStartingWithInternal(string keyPrefix, LoadStartingWithOperation operation, Stream stream = null, string matches = null,
            int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null,
            string startAfter = null, string transformer = null, CancellationToken token = default(CancellationToken))
        {
            var configuration = new LoadConfiguration();
            configure?.Invoke(configuration);

            operation.WithStartWith(keyPrefix, matches, start, pageSize, exclude, configure, startAfter);

            if (transformer != null)
                operation.WithTransformer(transformer, configuration.TransformerParameters);

            var command = operation.CreateRequest();
            if (command != null)
            {
                await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);

                if (stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }

            return command;
        }

        private async Task LoadAsyncInternal(string[] ids, Stream stream, LoadOperation operation,
            CancellationToken token = default(CancellationToken))
        {
            operation.ByIds(ids);

            var command = operation.CreateRequest();
            if (command != null)
            {
                await RequestExecutor.ExecuteAsync(command, Context, token).ConfigureAwait(false);

                if (stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }
        }

        public async Task LoadIntoStreamAsync(IEnumerable<string> ids, Stream output ,CancellationToken token = default(CancellationToken))
        {
            await LoadAsyncInternal(ids.ToArray(), output, new LoadOperation(this), token).ConfigureAwait(false);
        }

        public async Task LoadIntoStreamAsync<TTransformer>(IEnumerable<string> ids, Stream output,
            Action<ILoadConfiguration> configure = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new()
        {
            await LoadUsingTransformerInternalAsync(ids.ToArray(), output, new LoadTransformerOperation(this),
                new TTransformer().TransformerName, configure, token).ConfigureAwait(false);
        }

        public async Task LoadIntoStreamAsync(IEnumerable<string> ids, string transformer, Stream output,
            Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken))
        {
            await LoadUsingTransformerInternalAsync(ids.ToArray(), output, new LoadTransformerOperation(this),
                transformer, configure, token).ConfigureAwait(false);
        }

        public async Task LoadIntoStreamAsync(IEnumerable<string> ids, Type transformerType, Stream output,
            Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken))
        {
            var transformer = ((AbstractTransformerCreationTask)Activator.CreateInstance(transformerType)).TransformerName;
            await LoadUsingTransformerInternalAsync(ids.ToArray(), output, new LoadTransformerOperation(this),
                transformer, configure, token).ConfigureAwait(false);
        }

    }
}