using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.Operations;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        /// <inheritdoc />
        public async Task<T> LoadAsync<T>(string id, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (id == null)
                    return default;

                var loadOperation = new LoadOperation(this);
                loadOperation.ById(id);

                var command = loadOperation.CreateRequest();
                if (command != null)
                {
                    await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                    loadOperation.SetResult(command.Result);
                }

                return loadOperation.GetDocument<T>();
            }
        }

        /// <inheritdoc />
        public async Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            var loadOperation = new LoadOperation(this);
            await LoadAsyncInternal(ids.ToArray(), null, loadOperation, token).ConfigureAwait(false);

            return loadOperation.GetDocuments<T>();
        }

        /// <inheritdoc />
        public async Task<T> LoadAsync<T>(string id, Action<IIncludeBuilder<T>> includes, CancellationToken token = default)
        {
            if (id == null)
                return default;

            var result = await LoadAsync(new[] { id }, includes, token).ConfigureAwait(false);

            return result.Values.FirstOrDefault();
        }

        /// <inheritdoc />
        public Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, Action<IIncludeBuilder<T>> includes, CancellationToken token = default)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            if (includes == null)
                return LoadAsync<T>(ids, token);

            var includeBuilder = new IncludeBuilder<T>(Conventions);
            includes.Invoke(includeBuilder);

            return LoadAsyncInternal<T>(
                ids.ToArray(),
                includeBuilder.DocumentsToInclude?.ToArray(),
                includeBuilder.CountersToInclude?.ToArray(),
                includeBuilder.AllCounters,
                includeBuilder.TimeSeriesToInclude,
                includeBuilder.CompareExchangeValuesToInclude?.ToArray(),
                token);
        }

        /// <inheritdoc />
        public async Task<Dictionary<string, T>> LoadAsyncInternal<T>(string[] ids, string[] includes, string[] counterIncludes = null, bool includeAllCounters = false, IEnumerable<AbstractTimeSeriesRange> timeSeriesIncludes = null, string[] compareExchangeValueIncludes = null, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (ids == null)
                    throw new ArgumentNullException(nameof(ids));

                var loadOperation = new LoadOperation(this);
                loadOperation.ByIds(ids);
                loadOperation.WithIncludes(includes);

                if (includeAllCounters)
                {
                    loadOperation.WithAllCounters();
                }
                else
                {
                    loadOperation.WithCounters(counterIncludes);
                }
                
                loadOperation.WithTimeSeries(timeSeriesIncludes);
                loadOperation.WithCompareExchange(compareExchangeValueIncludes);

                var command = loadOperation.CreateRequest();
                if (command != null)
                {
                    await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: _sessionInfo, token: token).ConfigureAwait(false);
                    loadOperation.SetResult(command.Result);
                }

                return loadOperation.GetDocuments<T>();
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> LoadStartingWithAsync<T>(
            string idPrefix,
            string matches = null,
            int start = 0,
            int pageSize = 25,
            string exclude = null,
            string startAfter = null,
            CancellationToken token = default)
        {
            var operation = new LoadStartingWithOperation(this);
            await LoadStartingWithInternal(idPrefix, operation, null, matches, start,
                pageSize, exclude, startAfter, token).ConfigureAwait(false);

            return operation.GetDocuments<T>();
        }

        /// <inheritdoc />
        public async Task LoadStartingWithIntoStreamAsync(
            string idPrefix,
            Stream output,
            string matches = null,
            int start = 0,
            int pageSize = 25,
            string exclude = null,
            string startAfter = null,
            CancellationToken token = default)
        {
            if (output == null)
                throw new ArgumentNullException(nameof(output));

            await LoadStartingWithInternal(idPrefix, new LoadStartingWithOperation(this), output, matches, start, pageSize, exclude, startAfter, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task LoadIntoStreamAsync(IEnumerable<string> ids, Stream output, CancellationToken token = default)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (output == null)
                throw new ArgumentNullException(nameof(output));

            await LoadAsyncInternal(ids.ToArray(), output, new LoadOperation(this), token).ConfigureAwait(false);
        }

        private async Task LoadStartingWithInternal(
            string idPrefix,
            LoadStartingWithOperation operation,
            Stream stream = null,
            string matches = null,
            int start = 0,
            int pageSize = 25,
            string exclude = null,
            string startAfter = null,
            CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (idPrefix == null)
                    throw new ArgumentNullException(nameof(idPrefix));

                operation.WithStartWith(idPrefix, matches, start, pageSize, exclude, startAfter);

                var command = operation.CreateRequest();
                if (command != null)
                {
                    await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);

                    if (stream != null)
                        Context.Write(stream, command.Result.Results.Parent);
                    else
                        operation.SetResult(command.Result);
                }
            }
        }

        private async Task LoadAsyncInternal(string[] ids, Stream stream, LoadOperation operation, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (ids == null)
                    throw new ArgumentNullException(nameof(ids));

                operation.ByIds(ids);

                var command = operation.CreateRequest();
                if (command != null)
                {
                    await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);

                    if (stream != null)
                        Context.Write(stream, command.Result.Results.Parent);
                    else
                        operation.SetResult(command.Result);
                }
            }
        }
    }
}
