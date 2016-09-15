using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Platform;

namespace Raven.Client.Connection
{
    public class Operation
    {
        private readonly TaskCompletionSource<IOperationResult> _result = new TaskCompletionSource<IOperationResult>();
        private readonly RavenClientWebSocket _webSocket;
        private readonly CancellationToken _token;
        private readonly DocumentConvention _convention;

        private static readonly ILog logger = LogManager.GetLogger(typeof(Operation));

        public Operation(long id)
        {
            throw new NotImplementedException();
        }

        public Operation(RavenClientWebSocket webSocket, DocumentConvention convention, CancellationToken token)
        {
            _webSocket = webSocket;
            _token = token;
            _convention = convention;

            Task.Run(Receive);
        }

        public Action<IOperationProgress> OnProgressChanged;


        private async Task Receive()
        {
            try
            {
                using (var ms = new MemoryStream()) //TODO: consider merge this code as we have dupliate now
                {
                    ms.SetLength(4096);
                    while (_webSocket.State == WebSocketState.Open)
                    {
                        if (ms.Length > 4096*16)
                            ms.SetLength(4096);

                        ms.Position = 0;
                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(bytes.Array, (int) ms.Position, (int) (ms.Length - ms.Position)), _token);
                        ms.Position = result.Count;
                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
                            return;
                        }

                        while (result.EndOfMessage == false)
                        {
                            if (ms.Length - ms.Position < 1024)
                                ms.SetLength(ms.Length + 4096);
                            ms.TryGetBuffer(out bytes);
                            result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(bytes.Array, (int) ms.Position, (int) (ms.Length - ms.Position)), _token);
                            ms.Position += result.Count;
                        }

                        ms.SetLength(ms.Position);
                        ms.Position = 0;

                        using (var reader = new StreamReader(ms, Encoding.UTF8, true, 1024, true))
                        using (var jsonReader = new RavenJsonTextReader(reader)
                        {
                            SupportMultipleContent = true
                        })
                            while (jsonReader.Read())
                            {
                                var notification = _convention.CreateSerializer().Deserialize<OperationStatusChangeNotification>(jsonReader);
                                HandleReceivedNotification(notification);
                            }
                    }
                }
            }
            catch (WebSocketException ex)
            {
                logger.DebugException("Failed to receive a message, client was probably disconnected", ex);
                _result.SetException(ex);
            }
            finally
            {
                 _webSocket.Dispose();
            }
        }

        private void HandleReceivedNotification(OperationStatusChangeNotification notification)
        {
            var onProgress = OnProgressChanged;

            switch (notification.State.Status)
            {
                case OperationStatus.InProgress:
                    if (onProgress != null && notification.State.Progress != null)
                    {
                        onProgress(notification.State.Progress);
                    }
                    break;
                case OperationStatus.Completed:
                    _result.SetResult(notification.State.Result);
                    break;
                case OperationStatus.Faulted:
                    var exceptionResult = notification.State.Result as OperationExceptionResult;
                    if(exceptionResult?.StatusCode == 409)
                        _result.SetException(new DocumentInConflictException(exceptionResult.Message));
                    else
                        _result.SetException(new InvalidOperationException(exceptionResult?.Message));
                    break;
                case OperationStatus.Canceled:
                    _result.SetException(new OperationCanceledException());
                    break;
            }
        }

        public virtual Task<IOperationResult> WaitForCompletionAsync()
        {
            return _result.Task;
        }

        public virtual IOperationResult WaitForCompletion()
        {
            return AsyncHelpers.RunSync(WaitForCompletionAsync);
        }
    }
}
