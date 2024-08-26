using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using JetBrains.Annotations;
using NLog.Common;
using NLog.Layouts;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Global;

namespace Raven.Server.Logging;

public sealed class AdminLogsTarget : AbstractTarget
{
    private static readonly ConcurrentSet<WebSocket> Listeners = new();

    public static readonly AdminLogsTarget Instance;

    private static int RegisteredSockets;

    static AdminLogsTarget()
    {
        var layout = new JsonLayout();
        foreach (var attribute in Constants.Logging.DefaultAdminLogsJsonAttributes)
            layout.Attributes.Add(attribute);

        Instance = new()
        {
            Layout = layout
        };
    }

    private AdminLogsTarget()
    {
    }

    public static async Task RegisterAsync(WebSocket source, CancellationToken token)
    {
        using (RegisterInternal(RavenLogManagerServerExtensions.AdminLogsRule, source, Listeners, ref RegisteredSockets))
        {
            var arraySegment = new ArraySegment<byte>(new byte[512]);
            var buffer = new StringBuilder();
            var charBuffer = new char[Encodings.Utf8.GetMaxCharCount(arraySegment.Count)];
            while (token.IsCancellationRequested == false)
            {
                buffer.Length = 0;
                WebSocketReceiveResult result;
                do
                {
                    result = await source.ReceiveAsync(arraySegment, token).ConfigureAwait(false);
                    if (result.CloseStatus != null)
                    {
                        return;
                    }
                    var chars = Encodings.Utf8.GetChars(arraySegment.Array, 0, result.Count, charBuffer, 0);
                    buffer.Append(charBuffer, 0, chars);
                } while (result.EndOfMessage == false);
            }
        }
    }

    private readonly List<Task> _tasks = new();

    private readonly List<WebSocket> _sockets = new();

    private readonly StringBuilder _buffer = new();

    private readonly MemoryStream _memoryStream = new();

    private readonly char[] _transformBuffer = new char[4096];

    protected override void Write(AsyncLogEventInfo logEvent)
    {
        if (Listeners.IsEmpty)
            return;

        using (var context = GetContext())
        {
            Layout.Render(logEvent.LogEvent, context.Buffer);
            CopyToStream(context.Buffer, context.Stream, Encoding.UTF8, context.TransformBuffer);

            SendToWebSockets(context.Stream, context.Sockets, context.Tasks);
        }
    }

    private WriteContext GetContext() => new(_memoryStream, _buffer, _transformBuffer, _sockets, _tasks);

    private static void SendToWebSockets(MemoryStream stream, List<WebSocket> sockets, List<Task> tasks)
    {
        var toWrite = new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Length);

        foreach (var socket in Listeners)
        {
            try
            {
                var sendTask = socket.SendAsync(toWrite, WebSocketMessageType.Text, true, CancellationToken.None);

                sockets.Add(socket);
                tasks.Add(sendTask);
            }
            catch (Exception e)
            {
                RemoveWebSocket(socket, e.ToString());
            }
        }

        try
        {
            if (Task.WhenAll(tasks).Wait(250))
                return;
        }
        catch
        {
            // ignored
        }

        for (int i = 0; i < tasks.Count; i++)
        {
            var task = tasks[i];
            string error = null;
            if (task.IsFaulted)
            {
                error = task.Exception?.ToString() ?? "Faulted";
            }
            else if (task.IsCanceled)
            {
                error = "Canceled";
            }
            else if (task.IsCompleted == false)
            {
                error = "Timeout - 250 milliseconds";
            }

            if (error != null)
                RemoveWebSocket(sockets[i], error);
        }

        void RemoveWebSocket(WebSocket socket, string cause)
        {
            try
            {
                //To release the socket.ReceiveAsync call in Register function we must to close the socket 
                socket.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, cause, CancellationToken.None)
                    .ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
            }
            catch
            {
                // ignored
            }

            Listeners.TryRemove(socket);
        }
    }

    private readonly struct WriteContext : IDisposable
    {
        public readonly MemoryStream Stream;
        public readonly StringBuilder Buffer;
        public readonly char[] TransformBuffer;
        public readonly List<WebSocket> Sockets;
        public readonly List<Task> Tasks;

        public WriteContext(
            [NotNull] MemoryStream stream,
            [NotNull] StringBuilder buffer,
            [NotNull] char[] transformBuffer,
            [NotNull] List<WebSocket> sockets,
            [NotNull] List<Task> tasks)
        {
            Stream = stream ?? throw new ArgumentNullException(nameof(stream));
            Buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            TransformBuffer = transformBuffer ?? throw new ArgumentNullException(nameof(transformBuffer));
            Sockets = sockets ?? throw new ArgumentNullException(nameof(sockets));
            Tasks = tasks ?? throw new ArgumentNullException(nameof(tasks));
        }

        public void Dispose()
        {
            Stream.SetLength(0);
            Buffer.Clear();
            Tasks.Clear();
            Sockets.Clear();
        }
    }
}
