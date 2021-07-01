using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Client.Util;
using rvn.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace rvn
{
    public class LogTrafficWatch : IDisposable
    {
        private readonly int _pid;
        private ClientWebSocket _client;
        private Timer _serverAliveCheck;
        private int _exitCode;
        private const int ServerAliveCheckInterval = 5000;
        private CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly string _url;
        private readonly X509Certificate2 _cert;
        private readonly string _path;
        private readonly List<TrafficWatchChangeType> _changeTypes;

        public LogTrafficWatch(int? pid, string url, string certPath, string path, List<TrafficWatchChangeType>? changeTypes)
        {
            _pid = pid ?? ServerProcessUtil.GetRavenServerPid();
            _path = path;
            _changeTypes = changeTypes;
            if (string.IsNullOrEmpty(certPath))
            {
                if (url.StartsWith("https://"))
                {
                    ExitAndPrintError(ex: null, Environment.NewLine + $"URL '{url}' starts with 'https' but no certificate provided.");
                }
                _url = url.StartsWith("http://") == false ? $"http://{url}" : url;
            }
            else
            {
                if (url.StartsWith("http://"))
                {
                    ExitAndPrintError(ex: null, Environment.NewLine + $"URL '{url}' starts with 'http' but certificate provided.");
                }
                _cert = new X509Certificate2(certPath);
                _url = url.StartsWith("https://") == false ? $"https://{url}" : url;
            }
        }

        public void Stop()
        {
            Dispose();
            Console.WriteLine("Stop collection traffic watch. Exiting..");
            Environment.Exit(_exitCode);
        }

        public async Task Connect()
        {
            _serverAliveCheck = new Timer((s) =>
            {
                if (ServerProcessUtil.IsServerDown(_pid) == false)
                    return;

                Console.WriteLine($"RavenDB server process PID {_pid} exited.");
                _exitCode = 1;
                Stop();
            }, null, 0, ServerAliveCheckInterval);
            try
            {
                var url = new Uri($"{_url}/admin/traffic-watch"
                    .ToLower()
                    .ToWebSocketPath(), UriKind.Absolute);

                _client = new ClientWebSocket();
                if (_cert != null)
                    _client.Options.ClientCertificates.Add(_cert);

                try
                {
                    await _client.ConnectAsync(url, _cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ExitAndPrintError(ex, Environment.NewLine + "Couldn't connect to " + _url);
                }

                Console.WriteLine($"Connected to RavenDB server. Collecting traffic watch entries to {_path}");

                var maxFileSize = 128 * 1024 * 1024;
                var c = 0;
                while (true)
                {
                    string file = GetFileName();
                    var fileStream = SafeFileStream.Create(file, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, false);
                    try
                    {
                        if (_cts.IsCancellationRequested)
                            break;

                        var state = new JsonParserState();

                        using (var context = JsonOperationContext.ShortTermSingleUse())
                        using (var stream = new WebSocketStream(_client, _cts.Token))
                        using (context.GetManagedBuffer(out var buffer))
                        using (var parser = new UnmanagedJsonParser(context, state, "trafficwatch/receive"))
                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readObject/singleResult", parser, state))
                        using (var peepingTomStream = new PeepingTomStream(stream, context))
                        {
                            while (true)
                            {
                                if (_cts.IsCancellationRequested)
                                    break;

                                builder.Reset();
                                builder.Renew("trafficwatch/receive", BlittableJsonDocumentBuilder.UsageMode.None);

                                if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                                    continue;

                                await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);
                                using (var json = builder.CreateReader())
                                {
                                    if (_changeTypes != null)
                                    {
                                        if (json.TryGetMember("Type", out var obj) == false)
                                            continue;
                                        if (obj is LazyStringValue lzv && Enum.TryParse(lzv.ToString(), out TrafficWatchChangeType type))
                                        {
                                            if (_changeTypes.Contains(type) == false)
                                                continue;
                                        }
                                        else
                                        {
                                            continue;
                                        }
                                    }

                                    // log to file
                                    json.WriteJsonTo(fileStream);
                                    await fileStream.FlushAsync();
                                }

                                if (new FileInfo(file).Length >= maxFileSize)
                                {
                                    var oldFileName = file;
                                    file = GetFileName();

                                    var oldFileStream = fileStream;
                                    oldFileStream.Dispose();

                                    fileStream = SafeFileStream.Create(file, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, false);

                                    var t = Task.Run(() =>
                                    {
                                        using (var logStream = SafeFileStream.Create(oldFileName, FileMode.Open, FileAccess.Read))
                                        using (var newFileStream = SafeFileStream.Create(oldFileName + ".gz", FileMode.Create, FileAccess.Write))
                                        using (var compressionStream = new GZipStream(newFileStream, CompressionMode.Compress))
                                        {
                                            logStream.CopyTo(compressionStream);
                                        }

                                        File.Delete(oldFileName);
                                    });
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (c++ <= 100)
                        {
                            await Task.Delay(60000);
                            continue;
                        }

                        ExitAndPrintError(e, Environment.NewLine + "Couldn't write traffic watch entries");
                    }
                    finally
                    {
                        fileStream.Dispose();
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // closing
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
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

        private string GetFileName()
        {
            var d = DateTime.UtcNow.GetDefaultRavenFormat(isUtc: true).Replace(':', '-');
            var file = $"{Path.Combine(_path, $"trafficwatch-{d}.log")}";
            return file;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _client?.Dispose();
            _serverAliveCheck?.Dispose();
            _cts.Dispose();

            _client = null;
            _serverAliveCheck = null;
            _cts = null;
        }
    }
}
