using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;

namespace Raven.Client.Documents.BulkInsert;

internal class BulkInsertWriter : BulkInsertWriterBase
{
    private StreamWriter[] _writers = new StreamWriter[2];

    public BulkInsertWriter(JsonOperationContext ctx, CancellationToken token) : base(ctx, token)
    {

    }

    public StreamWriter StreamWriter { get; private set; }

    protected override void OnCurrentWriteStreamSet(MemoryStream currentWriteStream)
    {
        if (_writers[0]?.BaseStream == currentWriteStream)
            StreamWriter = _writers[0];
        else if (_writers[1]?.BaseStream == currentWriteStream)
            StreamWriter = _writers[1];
        else if (_writers[0] == null)
            StreamWriter = _writers[0] = new StreamWriter(currentWriteStream);
        else if (_writers[1] == null)
            StreamWriter = _writers[1] = new StreamWriter(currentWriteStream);
        else
            ThrowUnexpectedWriteStream();
    }

    public override async Task FlushIfNeeded(bool force = false)
    {
        await StreamWriter.FlushAsync().ConfigureAwait(false);

        await base.FlushIfNeeded(force).ConfigureAwait(false);
    }

    public void Write(string value)
    {
        StreamWriter.Write(value);
    }

    public void Write(char value)
    {
        StreamWriter.Write(value);
    }

    public void Write(long value)
    {
        StreamWriter.Write(value);
    }

    public override async ValueTask DisposeAsync()
    {
        await StreamWriter.FlushAsync().ConfigureAwait(false);

        await base.DisposeAsync().ConfigureAwait(false);
    }

    public Task FlushAsync()
    {
        return StreamWriter.FlushAsync();
    }

    private static void ThrowUnexpectedWriteStream()
    {
        throw new InvalidOperationException("We got stream for which we don't have the stream writer defined");
    }
}
