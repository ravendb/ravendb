using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace rvn
{
    public class LogStream
    {
        private readonly string _serverUrl;

        private readonly string _certPath;

        private readonly CancellationTokenSource _cancellationTokenSource;

        public LogStream(string serverUrl, string certPath)
        {
            _serverUrl = serverUrl;
            _certPath = certPath;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

        public async Task PrintAsync(Stream stream)
        {
            var ws = GetClient();
            var uri = GetLogsEndpoint();
            await ws.ConnectAsync(uri, _cancellationTokenSource.Token);

            using (var writer = new StreamWriter(stream))
            {
                writer.WriteLine("Connected to the server.");

                var buffer = new ArraySegment<byte>(new byte[8192]);
                while (true)
                {
                    using (var memStream = new MemoryStream())
                    {
                        WebSocketReceiveResult wsReceiveResult;
                        do
                        {
                            wsReceiveResult = await ws.ReceiveAsync(buffer, _cancellationTokenSource.Token);
                            memStream.Write(buffer.Array, buffer.Offset, wsReceiveResult.Count);
                        }
                        while (!wsReceiveResult.EndOfMessage);

                        memStream.Seek(0, SeekOrigin.Begin);

                        if (wsReceiveResult.MessageType == WebSocketMessageType.Text)
                        {
                            using (var reader = new StreamReader(memStream, Encoding.UTF8))
                                writer.Write(reader.ReadToEnd());
                        }

                        if (wsReceiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            writer.WriteLine($"Websocket connection closed: {wsReceiveResult.CloseStatus}, {wsReceiveResult.CloseStatusDescription}.");
                            break;
                        }
                    }
                }
            }
        }

        private Uri GetLogsEndpoint()
        {
            var adminLogsUriString = $"{_serverUrl.TrimEnd('/')}/admin/logs/watch";
            if (Uri.TryCreate(adminLogsUriString, UriKind.Absolute, out Uri result) == false)
                throw new ArgumentException($"Invalid server URL: {_serverUrl}");

            return result;
        }

        private ClientWebSocket GetClient()
        {
            if (string.IsNullOrEmpty(_certPath))
                return new ClientWebSocket();

            var cert = LoadCert();
            return new ClientWebSocket()
            {
                Options =
                {
                    ClientCertificates = cert
                }
            };
        }

        private X509CertificateCollection LoadCert()
        {
            var cert = X509Certificate.CreateFromCertFile(_certPath);
            var collection = new X509CertificateCollection(new []{ cert });
            return collection;
        }
    }
}
