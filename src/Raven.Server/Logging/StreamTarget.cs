using System;
using System.IO;
using System.Text;
using JetBrains.Annotations;
using NLog;
using Sparrow.Collections;
using Sparrow.Global;

namespace Raven.Server.Logging;

public class StreamTarget : AbstractTarget
{
    private static readonly ReadOnlyMemory<byte> HeaderBytes = Encoding.UTF8.GetBytes(Constants.Logging.DefaultHeaderAndFooterLayout + Environment.NewLine);

    private static readonly ConcurrentSet<Stream> Streams = new();

    public static readonly StreamTarget Instance = new()
    {
        Layout = Constants.Logging.DefaultLayout
    };

    private static readonly TargetCountGuardian RegisteredSockets = new();

    private StreamTarget()
    {
    }

    private readonly StringBuilder _buffer = new();

    private readonly MemoryStream _memoryStream = new();

    private readonly char[] _transformBuffer = new char[4096];

    public static IDisposable Register(Stream stream)
    {
        stream.Write(HeaderBytes.Span);

        return RegisterInternal(RavenLogManagerServerExtensions.PipeRule, stream, Streams, RegisteredSockets);
    }

    protected override void Write(LogEventInfo logEvent)
    {
        if (Streams.IsEmpty)
            return;

        using (var context = GetContext())
        {
            Layout.Render(logEvent, context.Buffer);
            CopyToStream(context.Buffer, context.Stream, Encoding.UTF8, context.TransformBuffer);

            var toWrite = new ReadOnlySpan<byte>(context.Stream.GetBuffer(), 0, (int)context.Stream.Length);

            foreach (var stream in Streams)
            {
                try
                {
                    stream.Write(toWrite);
                    stream.Flush();
                }
                catch
                {
                    Streams.TryRemove(stream);
                }
            }
        }
    }

    private WriteContext GetContext() => new(_memoryStream, _buffer, _transformBuffer);

    private readonly struct WriteContext : IDisposable
    {
        public readonly MemoryStream Stream;
        public readonly StringBuilder Buffer;
        public readonly char[] TransformBuffer;

        public WriteContext(
            [NotNull] MemoryStream stream,
            [NotNull] StringBuilder buffer,
            [NotNull] char[] transformBuffer)
        {
            Stream = stream ?? throw new ArgumentNullException(nameof(stream));
            Buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            TransformBuffer = transformBuffer ?? throw new ArgumentNullException(nameof(transformBuffer));
        }

        public void Dispose()
        {
            Stream.SetLength(0);
            Buffer.Clear();
        }
    }
}
