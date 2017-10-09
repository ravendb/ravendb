using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator_V2: AbstractMigrator
    {
        private readonly HttpClient _client;

        public Migrator_V2(
            string serverUrl,
            string databaseName,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            DocumentDatabase database,
            HttpClient client,
            OperationCancelToken cancelToken)
            : base(serverUrl, databaseName, result, onProgress, database, cancelToken)
        {
            _client = client;
        }

        public override async Task Execute()
        {
            await MigrateDocuments();

            await MigrateIndexes();
        }

        private async Task MigrateIndexes()
        {
            var url = $"{ServerUrl}/databases/{DatabaseName}/indexes";
            var request = new HttpRequestMessage(HttpMethod.Get, url);

            var response = await _client.SendAsync(request, CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export indexes from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            // indexes endpoint returns an array
            using (var indexesStream = new IndexesStream(responseStream))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(indexesStream, context))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide();
                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, Result, OnProgress, CancelToken.Token);

                smuggler.Execute(ensureProcessed: false);
            }
        }

        private async Task MigrateDocuments()
        {
            var url = $"{ServerUrl}/databases/{DatabaseName}/streams/docs";
            var request = new HttpRequestMessage(HttpMethod.Get, url);

            var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export documents from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(responseStream, context))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide();
                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, Result, OnProgress, CancelToken.Token);

                smuggler.Execute();
            }
        }

        public override void Dispose()
        {
            _client.Dispose();
        }

        private class IndexesStream : Stream
        {
            private Stream _baseStream;
            private readonly long _length;
            private long _position;

            private readonly MemoryStream _beginningStream =
                new MemoryStream(Encoding.UTF8.GetBytes("{ \"Indexes\" : "));
            private readonly MemoryStream _endingStream =
                new MemoryStream(Encoding.UTF8.GetBytes("}"));

            public IndexesStream(Stream baseStream)
            {
                if (baseStream == null)
                    throw new ArgumentNullException(nameof(baseStream));
                if (baseStream.CanRead == false)
                    throw new ArgumentException("can't read base stream");
                if (baseStream.CanSeek == false)
                    throw new ArgumentException("can't seek in base stream");

                _baseStream = baseStream;
                _length = _beginningStream.Length + baseStream.Length + _endingStream.Length;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                CheckDisposed();

                var remaining = _length - _position;
                if (remaining <= 0)
                    return 0;

                if (remaining < count)
                    count = (int)remaining;

                int read;
                if (_beginningStream.Position < _beginningStream.Length)
                {
                    read = _beginningStream.Read(buffer, offset, count);
                }
                else
                {
                    read = _baseStream.Read(buffer, offset, count);
                    if (read == 0)
                    {
                        read = _endingStream.Read(buffer, offset, count);
                    }
                }

                _position += read;
                return read;
            }

            private void CheckDisposed()
            {
                if (_baseStream == null)
                    throw new ObjectDisposedException(GetType().Name);
            }

            public override long Length
            {
                get
                {
                    CheckDisposed();
                    return _length;
                }
            }

            public override bool CanRead
            {
                get
                {
                    CheckDisposed();
                    return true;
                }
            }

            public override bool CanWrite
            {
                get
                {
                    CheckDisposed();
                    return false;
                }
            }

            public override bool CanSeek
            {
                get
                {
                    CheckDisposed();
                    return false;
                }
            }

            public override long Position
            {
                get
                {
                    CheckDisposed();
                    return _position;
                }
                set => throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Flush()
            {
                CheckDisposed();
                _baseStream.Flush();
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if (disposing == false)
                    return;

                // the caller is responsible for disposing the base stream
                _baseStream = null;
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }
        }
    }
}
