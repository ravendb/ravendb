using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Smuggler.Documents;

public sealed class OrchestratorStreamSource : StreamSource
{
    private readonly int _numberOfShards;

    public OrchestratorStreamSource(Stream stream, JsonOperationContext context, string databaseName, int numberOfShards, DatabaseSmugglerOptionsServerSide options = null) : base(stream, context, databaseName, options)
    {
        Mode = BlittableJsonDocumentBuilder.UsageMode.None;
        _numberOfShards = numberOfShards;
    }

    // use ConcurrentDictionary to prevent race between the dispose and release the stream due to reaching number of shards limit
    private ConcurrentDictionary<Slice, UniqueStreamValue> _uniqueStreams = new ConcurrentDictionary<Slice, UniqueStreamValue>(SliceComparer.Instance);

    public override Stream GetAttachmentStream(LazyStringValue hash, out string tag)
    {
        using (Slice.From(_allocator, hash, out var slice))
        {
            if (_uniqueStreams.TryGetValue(slice, out var item))
            {
                tag = "$from-sharding-import";
                return item.Stream.CreateDisposableReaderStream(new DisposableAction(() =>
                {
                    if (Interlocked.Increment(ref item.Usages) == _numberOfShards)
                    {
                        if (_uniqueStreams.TryRemove(slice, out var streamValue))
                        {
                            using (streamValue)
                            {
                                slice.Release(_allocator);
                            }
                        }
                    }
                }));
            }
        }
        tag = null;
        return null;
    }

    public override async Task<DocumentItem.AttachmentStream> ProcessAttachmentStreamAsync(JsonOperationContext context, BlittableJsonReaderObject data, INewDocumentActions actions)
    {
        var r = await base.ProcessAttachmentStreamAsync(context, data, actions);
        if (r.Stream is StreamsTempFile.InnerStream inner == false)
            throw new InvalidOperationException();

        if (_uniqueStreams.ContainsKey(r.Base64Hash) == false)
        {
            var hash = r.Base64Hash.Clone(_allocator);
            _uniqueStreams.TryAdd(hash, new UniqueStreamValue
            {
                Usages = 1,
                Stream = inner
            });
        }
            
        return r;
    }

    public override void Dispose()
    {
        foreach (var (key, _) in _uniqueStreams)
        {
            if (_uniqueStreams.TryRemove(key, out var streamValue))
            {
                using (streamValue)
                {
                    key.Release(_allocator);
                }
            }
        }

        // here we release the allocator
        base.Dispose();
    }

    private sealed class UniqueStreamValue : IDisposable
    {
        public StreamsTempFile.InnerStream Stream;
        public int Usages;

        public void Dispose()
        {
            Stream?.Dispose();
        }
    }
}
