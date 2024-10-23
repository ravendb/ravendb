using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Util;
using OperationCanceledException = System.OperationCanceledException;

namespace Raven.Debug.LogTrafficWatch
{
    public class TrafficWatchReplay : IDisposable
    {
        private readonly string[] _trafficFiles;
        private readonly string _schema;
        private readonly string _host;
        private readonly int _port;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly int _threads;
        
        public HttpClient HttpClient;
        private readonly Channel<TrafficWatchHttpChange> _trafficChannel = Channel.CreateBounded<TrafficWatchHttpChange>(1024);
        private readonly Task[] _consumers;
        private readonly Stopwatch _sp = new();


        public TrafficWatchReplay(string path, string certPath, string certPass, string host, int port, int threads = 8)
        {
            if (File.GetAttributes(path).HasFlag(FileAttributes.Directory) == false)
                ExitAndPrintError(null, $"'path' should be a directory. Received {path}");

            _trafficFiles = Directory.GetFiles(path, "*.*", SearchOption.TopDirectoryOnly);
            if (_trafficFiles.Length == 0)
                ExitAndPrintError(null, "Folder is empty");

            _schema = string.IsNullOrEmpty(certPath) ? "http" : "https";

            _threads = threads;
            _host = host;
            _port = port;
            _cancellationTokenSource = new CancellationTokenSource();
            _consumers = new Task[_threads];

            InitializeHttpClient(certPath, certPass);
        }

        public async Task Start()
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Start traffic watch replay");
            _sp.Start();

            var producers = new Task[_trafficFiles.Length];
            for (var index = 0; index < _trafficFiles.Length; index++)
            {
                var trafficFile = _trafficFiles[index];
                producers[index] = new TrafficParser(trafficFile, _trafficChannel, _cancellationTokenSource.Token).Execute();
            }

            for (var i = 0; i < _threads; i++)
            {
                _consumers[i] = new TrafficReplay(_trafficChannel, HttpClient, _cancellationTokenSource.Token, _schema, _host, _port).Execute();
            }

            var whenAllProducersTask = Task.WhenAll(producers);
            while (whenAllProducersTask.Wait(1_000) == false)
            {
                if (_cancellationTokenSource.Token.IsCancellationRequested)
                    break;

                Console.WriteLine($"[{DateTime.UtcNow:O}] Processed {TrafficReplay.RequestsCount:#,#;;0} requests");
            }

            _trafficChannel.Writer.Complete();
            await Task.WhenAll(_consumers);

            Console.WriteLine($"[{DateTime.UtcNow:O}] Totally processed {TrafficReplay.RequestsCount:#,#;;0} requests");
            Console.WriteLine($"[{DateTime.UtcNow:O}] Done after {_sp.Elapsed}");
        }

        public class TrafficParser
        {
            public static int LoadedRequestCount;
            public static JsonSerializerOptions JsonSerializerOptions = new()
            {
                Converters =
                {
                    new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
                }
            };

            private readonly string _file;
            private readonly Channel<TrafficWatchHttpChange> _trafficChannel;
            private readonly CancellationToken _cancellationToken;

            internal TrafficParser(string file, Channel<TrafficWatchHttpChange> trafficChannel, CancellationToken cancellationToken)
            {
                _file = file;
                _trafficChannel = trafficChannel;
                _cancellationToken = cancellationToken;
            }

            public async Task Execute()
            {
                try
                {
                    await foreach (var item in GetItemsList<TrafficWatchHttpChange>(_file, _cancellationToken))
                    {
                        await _trafficChannel.Writer.WriteAsync(item, _cancellationToken);
                        Interlocked.Increment(ref LoadedRequestCount);
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }

            protected virtual async IAsyncEnumerable<T> GetItemsList<T>(string fileName,
                [EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                await using var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read);

                await foreach (var item in JsonSerializer.DeserializeAsyncEnumerable<T>(fs, JsonSerializerOptions, cancellationToken: cancellationToken))
                {
                    if (item != null)
                        yield return item;
                }
            }
        }

        public class TrafficReplay
        {
            private readonly string _schema;
            private readonly string _host;
            private readonly int _port;
            private readonly Channel<TrafficWatchHttpChange> _trafficChannel;
            private readonly HttpClient _httpClient;
            private readonly CancellationToken _cancellationToken;

            public static int RequestsCount;

            internal TrafficReplay(Channel<TrafficWatchHttpChange> trafficChannel, HttpClient httpClient, CancellationToken cancellationToken, string schema = "http",
                string host = "127.0.0.1",
                int port = 8080)
            {
                _schema = schema;
                _host = host;
                _port = port;
                _trafficChannel = trafficChannel;
                _httpClient = httpClient;
                _cancellationToken = cancellationToken;
            }

            public async Task Execute()
            {
                var responseBuffer = new byte[4 * 1024];

                while (true)
                {
                    TrafficWatchHttpChange item;
                    try
                    {
                        item = await _trafficChannel.Reader.ReadAsync(_cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (ChannelClosedException)
                    {
                        // queue is empty
                        return;
                    }
                    
                    if (item.RequestUri == null) 
                        continue;
                    
                    var uri = UriReplace(item.RequestUri);

                    try
                    {
                        if (item.Type != TrafficWatchChangeType.Queries)
                            continue;

                        switch (item.HttpMethod)
                        {
                            case "GET":
                            {
                                // use the uri as is
                                using var getRequest = new HttpRequestMessage(HttpMethod.Get, uri);
                                using var response = await _httpClient.SendAsync(getRequest, _cancellationToken);
                                await ConsumeResponse(response, responseBuffer);
                                break;
                            }
                            case "POST":
                            {
                                string query;
                                string body;

                                var start = RegexIndexOf(item.CustomInfo, @"{"".0"":");

                                if (start > 0) //If request with params
                                {
                                    var postParams = item.CustomInfo.Substring(start);
                                    Dictionary<string, JsonElement> p = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(postParams);
                                    query = Regex.Replace(item.CustomInfo.Substring(0, start), @"""", "\\\"");
                                    string stringParameters = string.Join(Environment.NewLine,
                                        p.Select(x => $"\"{x.Key}\" : {ConvertJsonElement(x.Value)}"));
                                    body = $"{{\"Query\":\"{query}\",\"QueryParameters\":{{{stringParameters}}}}}";
                                }
                                else
                                {
                                    query = item.CustomInfo;
                                    body = $"{{\"Query\":\"{query}\"}}";
                                }

                                using var postRequest = new HttpRequestMessage(HttpMethod.Post, uri) { Content = new StringContent(body) };
                                using var response = await _httpClient.SendAsync(postRequest, _cancellationToken);
                                await ConsumeResponse(response, responseBuffer);
                                break;
                            }
                        }

                        Interlocked.Increment(ref RequestsCount);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }
            }

            private async Task ConsumeResponse(HttpResponseMessage response, byte[] responseBuffer)
            {
                await using var stream = await response.Content.ReadAsStreamAsync(_cancellationToken);
                while (await stream.ReadAsync(responseBuffer, _cancellationToken) > 0)
                {
                }
            }

            private Uri UriReplace(string original)
            {
                try
                {
                    return new UriBuilder(original) {
                        Host = _host,
                        Port = _port,
                        Scheme = _schema
                    }.Uri;

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    return null;
                }
            }

            private static string ConvertJsonElement(JsonElement e)
            {
                return e.ValueKind switch
                {
                    JsonValueKind.Number => e.ToString(),
                    JsonValueKind.Array => e.ToString(),
                    JsonValueKind.Object => e.ToString(),
                    _ => $"\"{e}\""
                };
            }
        }

        public static int RegexIndexOf(string str, string pattern)
        {
            var m = Regex.Match(str, pattern);
            return m.Success ? m.Index : -1;
        }


        private void InitializeHttpClient(string certPath, string certPass)
        {
            if (string.IsNullOrEmpty(certPath) == false)
            {
                HttpClient = new HttpClient(new HttpClientHandler
                {
                    ClientCertificates =
                    {
                        CertificateHelper.CreateCertificateFromPfx(certPath, certPass, X509KeyStorageFlags.MachineKeySet)
                    }
                });
                return;
            }

            HttpClient = new HttpClient();
        }

        private static void ExitAndPrintError(Exception ex, string errorText)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorText);
            Console.ResetColor();
            if (ex != null)
            {
                Console.WriteLine();
                Console.WriteLine(ex);
            }

            Environment.Exit(2);
        }

        public void Stop()
        {
            Console.WriteLine("Stop collection traffic watch. Exiting...");
            _cancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            HttpClient?.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }
}
