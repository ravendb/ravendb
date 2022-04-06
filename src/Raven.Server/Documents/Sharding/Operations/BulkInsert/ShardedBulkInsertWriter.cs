using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal class ShardedBulkInsertWriter : BulkInsertWriterBase
{
    private bool _first = true;
    private MemoryStream _currentWriter;

    public ShardedBulkInsertWriter(JsonOperationContext ctx, CancellationToken token)
        : base(ctx, token)
    {
    }

    protected override void OnCurrentWriteStreamSet(MemoryStream currentWriteStream)
    {
        _currentWriter = currentWriteStream;
    }

    public async Task WriteStreamAsync(Stream src)
    {
        if (_first == false)
            _currentWriter.WriteByte((byte)',');

        _first = false;

        await WriteToStreamAsync(src, _currentWriter);
    }

    public async Task WriteStreamDirectlyToRequestAsync(Stream src)
    {
        await WriteToRequestStreamAsync(src);
    }
}
