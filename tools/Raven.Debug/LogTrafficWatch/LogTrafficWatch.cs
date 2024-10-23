using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Debug.LogTrafficWatch
{
    public class LogTrafficWatch : IDisposable
    {
        private ClientWebSocket _client;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly LogNameCreator _logNameCreator;

        private readonly string _path;
        private readonly string _url;
        private readonly X509Certificate2 _cert;
        private readonly string _database;
        private readonly TrafficWatchChangeType[] _changeTypes;
        private readonly bool _verbose;
        private Task _runningTask;
        private bool _firstConnection = true;
        int _errorCount = 0;

        public LogTrafficWatch(string path, string url, string certPath, string certPass, string database, TrafficWatchChangeType[] changeTypes, bool verbose)
        {
            _database = database;
            _changeTypes = changeTypes;
            _verbose = verbose;
            _path = path;
            if (File.GetAttributes(path).HasFlag(FileAttributes.Directory) == false)
                throw new ArgumentException($"'path' should be a directory. Received {path}");

            var files = Directory.GetFiles(path, LogNameCreator.SearchPattern);
            var lastLog = files.LastOrDefault();
            if (lastLog != null)
            {
                (DateTime date, int number) = LogNameCreator.GetLogMetadataFromFileName(lastLog);
                if (date == DateTime.UtcNow.Date)
                    _logNameCreator = new LogNameCreator(number + 1);
            }
            _logNameCreator ??= new LogNameCreator(0);

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
                _cert = CertificateHelper.CreateCertificateFromPfx(certPath, certPass, X509KeyStorageFlags.MachineKeySet);
                _url = url.StartsWith("https://") == false ? $"https://{url}" : url;
            }
        }

        public void Stop()
        {
            Console.WriteLine("Stop collection traffic watch. Exiting...");
            Dispose();
            Environment.Exit(0);
        }

        public static string ToWebSocketPath(string path)
        {
            return path
                .Replace("http://", "ws://")
                .Replace("https://", "wss://")
                .Replace(".fiddler", "");
        }

        public async Task Connect()
        {

            while (true)
            {
                try
                {
                    _runningTask = ConnectInternal();
                    await _runningTask.ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (_firstConnection)
                        ExitAndPrintError(e, Environment.NewLine + "Couldn't connect to the server");

                    if (_errorCount++ <= 100)
                    {
                        Console.WriteLine($"Lost connection to the server... trying again - attempt No. {_errorCount}");
                        await Task.Delay(1000);
                        continue;
                    }

                    ExitAndPrintError(e, Environment.NewLine + "Couldn't write traffic watch entries");
                }
            }
        }

        public async Task ConnectInternal()
        {
            try
            {
                var urlBuilder = new StringBuilder(_url).Append("/admin/traffic-watch");

                if (string.IsNullOrWhiteSpace(_database) == false)
                    urlBuilder.Append("?resourceName=").Append(_database);

                var stringUrl = ToWebSocketPath(urlBuilder.ToString().ToLower());
                var url = new Uri(stringUrl, UriKind.Absolute);
                _client = new ClientWebSocket();
                if (_cert != null)
                    _client.Options.ClientCertificates.Add(_cert);

                await _client.ConnectAsync(url, _cancellationTokenSource.Token).ConfigureAwait(false);
                _firstConnection = false;

                Console.WriteLine($"Connected to RavenDB server. Collecting traffic watch entries to {_path}");

                const int maxFileSize = 128 * 1024 * 1024;
                while (_cancellationTokenSource.IsCancellationRequested == false)
                {
                    string file = Path.Combine(_path, _logNameCreator.GetNewFileName());

                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    using (context.AcquireParserState(out var state))
                    // Read
                    await using (var stream = new WebSocketStream(_client, _cancellationTokenSource.Token))
                    using (context.GetMemoryBuffer(out var buffer))
                    using (var parser = new UnmanagedJsonParser(context, state, "trafficwatch/receive"))
                    using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readObject/singleResult", parser, state))
                    // Write
                    await using (var fileStream = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, false))
                    await using (var gZipStream = new GZipStream(fileStream, CompressionMode.Compress, false))
                    using (var peepingTomStream = new PeepingTomStream(stream, context))
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, gZipStream))
                    {
                        writer.WriteStartArray();
                        var isFirst = true;

                        while (fileStream.Length < maxFileSize)
                        {
                            if (_cancellationTokenSource.IsCancellationRequested)
                            {
                                writer.WriteEndArray();
                                break;
                            }

                            try
                            {
                                var flushCount = 0;
                                while (fileStream.Length < maxFileSize && _cancellationTokenSource.IsCancellationRequested == false)
                                {
                                    builder.Reset();
                                    builder.Renew("trafficwatch/receive", BlittableJsonDocumentBuilder.UsageMode.None);

                                    if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                                        continue;

                                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);
                                    using (var json = builder.CreateReader())
                                    {
                                        if (_changeTypes != null)
                                        {
                                            if (json.TryGet("Type", out TrafficWatchChangeType type) == false)
                                                continue;
                                            if (_changeTypes.Contains(type) == false)
                                                continue;
                                        }

                                        if (_database != null)
                                        {
                                            if (json.TryGet("DatabaseName", out LazyStringValue databaseName) == false ||
                                               _database.Equals(databaseName, StringComparison.OrdinalIgnoreCase) == false)
                                                continue;
                                        }

                                        if (isFirst == false)
                                            writer.WriteComma();

                                        isFirst = false;
                                        if (_verbose)
                                            Console.WriteLine(json);

                                        writer.WriteObject(json);
                                        _errorCount = 0;
                                        if (flushCount++ % 128 == 0)
                                            await writer.FlushAsync();
                                    }
                                }
                            }
                            catch (Exception)
                            {
                                writer.WriteEndArray();
                                throw;
                            }
                        }
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // closing
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

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();

            var runningTask = _runningTask;
            if (runningTask.IsCanceled == false && runningTask.IsFaulted == false)
                _runningTask.Wait(TimeSpan.FromSeconds(20));

            try
            {
                _client?.Dispose();
            }
            catch
            {
                // ignored
            }

            _cancellationTokenSource.Dispose();

            _client = null;
        }

        internal class LogNameCreator
        {
            private const string LogExtension = ".log.gz";
            private const string DateFormat = "yyyy-MM-dd";
            private const string Prefix = "trafficwatch";
            private static readonly Regex _logNameRegex = new Regex($@"{Prefix}-(.{{{DateFormat.Length}}}).(\d+){LogExtension}");

            private int _currentLogNumber = 0;
            private DateTime _lastLogTime = DateTime.UtcNow.Date;

            public static readonly string SearchPattern = $"{Prefix}*{LogExtension}";

            public LogNameCreator(int startLogNumber)
            {
                _currentLogNumber = startLogNumber;
            }
            public string GetNewFileName()
            {
                if (DateTime.UtcNow.Date != _lastLogTime)
                {
                    _lastLogTime = DateTime.UtcNow.Date;
                    _currentLogNumber = 0;
                }

                var dateString = _lastLogTime.ToString(DateFormat, CultureInfo.InvariantCulture);
                return new StringBuilder(Prefix)
                    .Append('-')
                    .Append(dateString)
                    .Append('.')
                    .Append(_currentLogNumber++.ToString("000", CultureInfo.InvariantCulture))
                    .Append(LogExtension)
                    .ToString();
            }

            public static (DateTime Date, int Number) GetLogMetadataFromFileName(string fileName)
            {
                var match = _logNameRegex.Match(fileName);
                if (DateTime.TryParse(match.Groups[1].Value, out var dateTime) == false)
                    throw new InvalidOperationException($"Cant parse {fileName} with regex {_logNameRegex}");

                if (int.TryParse(match.Groups[2].Value, out var logNumber) == false)
                    throw new InvalidOperationException($"Cant parse {fileName} with regex {_logNameRegex}");

                return (dateTime, logNumber);
            }
        }
    }
}
