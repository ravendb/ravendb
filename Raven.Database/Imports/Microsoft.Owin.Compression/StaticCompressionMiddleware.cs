// <copyright file="StaticCompressionMiddleware.cs" company="Microsoft Open Technologies, Inc.">
// Copyright 2011-2013 Microsoft Open Technologies, Inc. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin.Compression.Encoding;
using Microsoft.Owin.Compression.Infrastructure;
using Microsoft.Owin.Compression.Storage;

namespace Microsoft.Owin.Compression
{
    public class StaticCompressionMiddleware
    {
        private readonly Func<IDictionary<string, object>, Task> _next;
        private readonly StaticCompressionOptions _options;

        private ICompressedStorage _storage;
        private bool _storageInitialized;
        private object _storageLock = new object();

        public StaticCompressionMiddleware(Func<IDictionary<string, object>, Task> next, StaticCompressionOptions options)
        {
            _next = next;
            _options = options;
        }

        public Task Invoke(IDictionary<string, object> environment)
        {
            IEncoding compression = SelectCompression(environment);
            if (compression == null)
            {
                return _next(environment);
            }

            ICompressedStorage storage = GetStorage(environment);
            if (storage == null)
            {
                return _next(environment);
            }

            var context = new StaticCompressionContext(environment, _options, compression, storage);
            context.Attach();
            return _next(environment)
                .Then((Func<Task>)context.Complete)
                .Catch(context.Complete);
        }

        private ICompressedStorage GetStorage(IDictionary<string, object> environment)
        {
            return LazyInitializer.EnsureInitialized(
                ref _storage,
                ref _storageInitialized,
                ref _storageLock,
                () => GetStorageOnce(environment));
        }

        private ICompressedStorage GetStorageOnce(IDictionary<string, object> environment)
        {
            ICompressedStorage storage = _options.CompressedStorageProvider.Create();
            var onAppDisposing = new OwinRequest(environment).Get<CancellationToken>("host.OnAppDisposing");
            if (onAppDisposing != CancellationToken.None)
            {
                onAppDisposing.Register(storage.Dispose);
            }
            return storage;
        }

        private IEncoding SelectCompression(IDictionary<string, object> environment)
        {
            var request = new OwinRequest(environment);

            var bestAccept = new Accept { Encoding = "identity", Quality = 0 };
            IEncoding bestEncoding = null;

            IList<string> acceptEncoding = request.Headers.GetValues("accept-encoding");
            if (acceptEncoding != null)
            {
                foreach (var segment in new HeaderSegmentCollection(acceptEncoding))
                {
                    if (!segment.Data.HasValue)
                    {
                        continue;
                    }
                    Accept accept = Parse(segment.Data.Value);
                    if (accept.Quality == 0 || accept.Quality < bestAccept.Quality)
                    {
                        continue;
                    }
                    IEncoding compression = _options.EncodingProvider.GetCompression(accept.Encoding);
                    if (compression == null)
                    {
                        continue;
                    }
                    bestAccept = accept;
                    bestEncoding = compression;
                    if (accept.Quality == 1000)
                    {
                        break;
                    }
                }
            }
            return bestEncoding;
        }

        private Accept Parse(string value)
        {
            string encoding = value;
            int quality = 1000;
            string detail = string.Empty;

            int semicolonIndex = value.IndexOf(';');
            if (semicolonIndex != -1)
            {
                encoding = value.Substring(0, semicolonIndex);
                detail = value.Substring(semicolonIndex + 1);
            }
            int qualityIndex = detail.IndexOf("q=", StringComparison.OrdinalIgnoreCase);
            if (qualityIndex != -1)
            {
                quality = (int)((double.Parse(detail.Substring(qualityIndex + 2)) * 1000) + .5);
            }
            return new Accept
            {
                Encoding = encoding.Trim(),
                Quality = quality
            };
        }

        private struct Accept
        {
            public string Encoding;
            public int Quality;
        }
    }
}
