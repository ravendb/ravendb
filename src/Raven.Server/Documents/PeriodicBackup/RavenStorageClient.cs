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

namespace Raven.Server.Documents.PeriodicBackup
{
    public abstract class RavenStorageClient : IDisposable
    {
        private readonly List<HttpClient> _clients = new List<HttpClient>();
        protected readonly CancellationToken CancellationToken;
        protected readonly Progress Progress;
        protected const int MaxRetriesForMultiPartUpload = 5;

        protected RavenStorageClient(Progress progress, CancellationToken? cancellationToken)
        {
            Debug.Assert(progress == null || (progress.UploadProgress != null && progress.OnUploadProgress != null));

            Progress = progress;
            CancellationToken = cancellationToken ?? CancellationToken.None;
        }

        protected HttpClient GetClient(TimeSpan? timeout = null)
        {
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.None
            };

            var client = new HttpClient(handler)
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

        public class Blob
        {
            public Blob(Stream data, Dictionary<string, string> metadata)
            {
                Data = data;
                Metadata = metadata;
            }

            public Stream Data { get; }

            public Dictionary<string, string> Metadata { get; }
        }
    }
}
