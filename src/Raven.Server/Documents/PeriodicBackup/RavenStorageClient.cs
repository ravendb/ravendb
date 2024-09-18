// -----------------------------------------------------------------------
//  <copyright file="RavenStorageClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents.PeriodicBackup
{
    public abstract class RavenStorageClient : IDisposable
    {
        private readonly List<RavenHttpClient> _clients = new();
        protected readonly CancellationToken CancellationToken;
        protected readonly Progress Progress;
        protected const int MaxRetriesForMultiPartUpload = 5;

        protected RavenStorageClient(Progress progress, CancellationToken? cancellationToken)
        {
            Debug.Assert(progress == null || (progress.UploadProgress != null && progress.OnUploadProgress != null));

            Progress = progress;
            CancellationToken = cancellationToken ?? CancellationToken.None;
        }

        protected RavenHttpClient GetClient(TimeSpan? timeout = null)
        {
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.None
            };

            var client = new RavenHttpClient(handler)
            {
                Timeout = timeout ?? TimeSpan.FromSeconds(120)
            };

            _clients.Add(client);

            return client;
        }

        public virtual void Dispose()
        {
            var exceptions = new List<Exception>();

            foreach (var client in _clients)
            {
                try
                {
                    client.Dispose();
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        public sealed class Blob : IDisposable
        {
            private IDisposable _toDispose;

            public Blob(Stream data, IDictionary<string, string> metadata, long sizeInBytes, IDisposable toDispose = null)
            {
                Data = data ?? throw new ArgumentNullException(nameof(data));
                Metadata = metadata;
                Size = new Size(sizeInBytes, SizeUnit.Bytes);
                _toDispose = toDispose;
            }

            public Stream Data { get; }

            public IDictionary<string, string> Metadata { get; }

            public Size Size { get; }

            public void Dispose()
            {
                _toDispose?.Dispose();
                _toDispose = null;
            }
        }

        public sealed class ListBlobResult
        {
            public IEnumerable<BlobProperties> List { get; set; }

            public string ContinuationToken { get; set; }
        }

        public sealed class BlobProperties
        {
            public string Name { get; set; }
            public DateTimeOffset? LastModified { get; set; }
        }
    }
}
