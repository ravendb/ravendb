using System;
using System.IO;
using System.Text;
using System.Threading;
using JetBrains.Annotations;
using NLog;
using Raven.Client.Util;
using Sparrow.Collections;
using Sparrow.Global;

namespace Raven.Server.Logging;

public class StreamTarget : AbstractTarget
{
    private static readonly ReadOnlyMemory<byte> HeaderBytes = Encoding.UTF8.GetBytes(Constants.Logging.DefaultServerHeaderAndFooterLayout + Environment.NewLine);

    private static readonly ConcurrentSet<Stream> Streams = new();

    public static readonly StreamTarget Instance = new()
    {
        Layout = Constants.Logging.DefaultServerLayout
    };

    private static readonly TargetCountGuardian RegisteredSockets = new();

    private static readonly Lock WriteLocker = new();

    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(5);

    private StreamTarget()
    {
    }

    private readonly StringBuilder _buffer = new();

    private readonly MemoryStream _memoryStream = new();

    private readonly char[] _transformBuffer = new char[4096];

    public static IDisposable Register(Stream stream)
    {
        stream.Write(HeaderBytes.Span);

        var registration = RegisterInternal(RavenLogManagerServerExtensions.PipeRule, stream, Streams, RegisteredSockets);

        return new DisposableAction(() =>
        {
            FlushPendingEntries();

            lock (WriteLocker)
                Streams.TryRemove(stream);

            registration.Dispose();
        });
    }

    private static void FlushPendingEntries()
    {
        foreach (var target in RavenLogManagerServerExtensions.PipeRule.Targets)
        {
            var flushed = new ManualResetEventSlim();
            target.Flush(_ => flushed.Set());
            flushed.Wait(FlushTimeout);
        }
    }

    protected override void Write(LogEventInfo logEvent)
    {
        if (Streams.IsEmpty)
            return;

        using (var context = GetContext())
        {
            Layout.Render(logEvent, context.Buffer);
            context.Buffer.Append(Environment.NewLine);
            CopyToStream(context.Buffer, context.Stream, Encoding.UTF8, context.TransformBuffer);

            var toWrite = new ReadOnlySpan<byte>(context.Stream.GetBuffer(), 0, (int)context.Stream.Length);

            lock (WriteLocker)
            {
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
