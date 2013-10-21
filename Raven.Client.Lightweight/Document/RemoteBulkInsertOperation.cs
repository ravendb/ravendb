#if !NETFX_CORE
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#else
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
#endif
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
#if !SILVERLIGHT
using System.IO.Compression;
#else
using Raven.Client.Connection.Async;
using Raven.Client.Silverlight.Connection;
using Ionic.Zlib;
#endif

namespace Raven.Client.Document
{

    public interface ILowLevelBulkInsertOperation : IDisposable
    {
        Guid OperationId { get; }

        void Write(string id, RavenJObject metadata, RavenJObject data);

        Task DisposeAsync();

        /// <summary>
        ///     Report on the progress of the operation
        /// </summary>
        event Action<string> Report;
    }

    public class RemoteBulkInsertOperation : ILowLevelBulkInsertOperation, IObserver<BulkInsertChangeNotification>
    {
        private CancellationTokenSource cancellationTokenSource;

#if !SILVERLIGHT
        private readonly ServerClient operationClient;
#else
		private readonly AsyncServerClient operationClient;
#endif
        private readonly IDatabaseChanges operationChanges;
        private readonly MemoryStream bufferedStream = new MemoryStream();
        private readonly BlockingCollection<RavenJObject> queue;

        private HttpJsonRequest operationRequest;
        private readonly Task operationTask;
        private int total;

#if !SILVERLIGHT
        public RemoteBulkInsertOperation(BulkInsertOptions options, ServerClient client, IDatabaseChanges changes)
#else
		public RemoteBulkInsertOperation(BulkInsertOptions options, AsyncServerClient client, IDatabaseChanges changes)
#endif
        {
            var synchronizationContext = SynchronizationContext.Current;
            try
            {
                SynchronizationContext.SetSynchronizationContext(null);

                OperationId = Guid.NewGuid();
                operationClient = client;
                operationChanges = changes;
                queue = new BlockingCollection<RavenJObject>(Math.Max(128, (options.BatchSize * 3) / 2));

                operationTask = StartBulkInsertAsync(options);
#if !MONO
                SubscribeToBulkInsertNotifications(changes);
#endif
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(synchronizationContext);
            }
        }

#if !MONO
        private void SubscribeToBulkInsertNotifications(IDatabaseChanges changes)
        {
            changes
                .ForBulkInsert(OperationId)
                .Subscribe(this);
        }
#endif

        private async Task StartBulkInsertAsync(BulkInsertOptions options)
        {
#if !SILVERLIGHT
            var expect100Continue = operationClient.Expect100Continue();
#endif
            var operationUrl = CreateOperationUrl(options);
            var token = await GetToken();
            try
            {
                token = await ValidateThatWeCanUseAuthenticateTokens(token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(
                    "Could not authenticate token for bulk insert, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }

            operationRequest = CreateOperationRequest(operationUrl, token);

            var stream = await operationRequest.GetRawRequestStream();

#if !SILVERLIGHT
            try
            {
                if (expect100Continue != null)
                    expect100Continue.Dispose();
            }
            catch
            {

            }
#endif
            var cancellationToken = CreateCancellationToken();
            await Task.Factory.StartNew(() => WriteQueueToServer(stream, options, cancellationToken), TaskCreationOptions.LongRunning);
        }

        private CancellationToken CreateCancellationToken()
        {
            cancellationTokenSource = new CancellationTokenSource();
            return cancellationTokenSource.Token;
        }

        private async Task<string> GetToken()
        {
            // this will force the HTTP layer to authenticate, meaning that our next request won't have to
            var jsonToken = await GetAuthToken();

            return jsonToken.Value<string>("Token");
        }

        private Task<RavenJToken> GetAuthToken()
        {

			var request = operationClient.CreateRequest("/singleAuthToken", "GET",
														disableRequestCompression: true);

			return request.ReadResponseJsonAsync();
        }

        private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
        {
			var request = operationClient.CreateRequest("/singleAuthToken", "GET", disableRequestCompression: true);

            request.DisableAuthentication();
            request.AddOperationHeader("Single-Use-Auth-Token", token);
            var result = await request.ReadResponseJsonAsync();
            return result.Value<string>("Token");
        }

        private HttpJsonRequest CreateOperationRequest(string operationUrl, string token)
        {
			var request = operationClient.CreateRequest(operationUrl, "POST", disableRequestCompression: true);

            request.DisableAuthentication();
            // the request may take a long time to process, so we need to set a large timeout value
            request.PrepareForLongRequest();
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            return request;
        }

        private string CreateOperationUrl(BulkInsertOptions options)
        {
            string requestUrl = "/bulkInsert?";
            if (options.CheckForUpdates)
                requestUrl += "checkForUpdates=true";
            if (options.CheckReferencesInIndexes)
                requestUrl += "&checkReferencesInIndexes=true";

            requestUrl += "&operationId=" + OperationId;

            return requestUrl;
        }

        private void WriteQueueToServer(Stream stream, BulkInsertOptions options, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batch = new List<RavenJObject>();
                RavenJObject document;
                while (queue.TryTake(out document, 200))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (document == null) // marker
                    {
                        FlushBatch(stream, batch);
                        return;
                    }

                    batch.Add(document);

                    if (batch.Count >= options.BatchSize)
                        break;
                }

                FlushBatch(stream, batch);
            }
        }

        public event Action<string> Report;

        public Guid OperationId { get; private set; }

        public void Write(string id, RavenJObject metadata, RavenJObject data)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (metadata == null) throw new ArgumentNullException("metadata");
            if (data == null) throw new ArgumentNullException("data");

            if (operationTask.IsCanceled || operationTask.IsFaulted)
                operationTask.Wait(); // error early if we have  any error

            metadata["@id"] = id;
            data[Constants.Metadata] = metadata;

            queue.Add(data);
        }

        private async Task<bool> IsOperationCompleted(long operationId)
        {
            var status = await GetOperationStatus(operationId);

            if (status == null)
                return true;

            if (status.Value<bool>("Completed"))
                return true;

            return false;
        }

        private Task<RavenJToken> GetOperationStatus(long operationId)
        {
#if !SILVERLIGHT
            return new CompletedTask<RavenJToken>(operationClient.GetOperationStatus(operationId));
#else
			return operationClient.GetOperationStatusAsync(operationId);
#endif
        }

        private volatile bool disposed;

        public async Task DisposeAsync()
        {
            if (disposed)
                return;
            disposed = true;
            queue.Add(null);
            await operationTask;

            operationTask.AssertNotFailed();

            ReportInternal("Finished writing all results to server");

            long operationId;

            using (var response = await operationRequest.RawExecuteRequestAsync())
            using (var stream = response.GetResponseStream())
            using (var streamReader = new StreamReader(stream))
            {
                var result = RavenJObject.Load(new JsonTextReader(streamReader));
                operationId = result.Value<long>("OperationId");
            }

            while (true)
            {
                if (await IsOperationCompleted(operationId))
                    break;

                Thread.Sleep(500);
            }

            ReportInternal("Done writing to server");
        }

        public void Dispose()
        {
            if (disposed)
                return;

            var disposeAsync = DisposeAsync().ConfigureAwait(false);
            disposeAsync.GetAwaiter().GetResult();
        }

        private void FlushBatch(Stream requestStream, ICollection<RavenJObject> localBatch)
        {
            if (localBatch.Count == 0)
                return;
            bufferedStream.SetLength(0);
            WriteToBuffer(localBatch);

            var requestBinaryWriter = new BinaryWriter(requestStream);
            requestBinaryWriter.Write((int)bufferedStream.Position);
            bufferedStream.WriteTo(requestStream);
            requestStream.Flush();

            total += localBatch.Count;
            Action<string> report = Report;
            if (report != null)
            {
                report(string.Format("Wrote {0:#,#} (total {2:#,#} documents to server gzipped to {1:#,#.##} kb",
                                     localBatch.Count,
                                     bufferedStream.Position / 1024,
                                     total));
            }
        }

        private void WriteToBuffer(ICollection<RavenJObject> localBatch)
        {
            using (var gzip = new GZipStream(bufferedStream, CompressionMode.Compress, leaveOpen: true))
            {
                var binaryWriter = new BinaryWriter(gzip);
                binaryWriter.Write(localBatch.Count);
                var bsonWriter = new BsonWriter(binaryWriter);
                foreach (var doc in localBatch)
                {
                    doc.WriteTo(bsonWriter);
                }

                bsonWriter.Flush();
                binaryWriter.Flush();
                gzip.Flush();
            }
        }

        private void ReportInternal(string format, params object[] args)
        {
            var onReport = Report;
            if (onReport != null)
                onReport(string.Format(format, args));
        }

        public void OnNext(BulkInsertChangeNotification value)
        {
            if (value.Type == DocumentChangeTypes.BulkInsertError)
            {
                cancellationTokenSource.Cancel();
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
#endif
