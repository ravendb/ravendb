using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Sparrow.Json;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchFileWriter : IDisposable
    {
        private readonly LogNameCreator _logNameCreator;
        private readonly string _file;
        private readonly JsonOperationContext _context = JsonOperationContext.ShortTermSingleUse();
        private readonly ConcurrentQueue<TrafficWatchChangeBase> _messages = new();
        private readonly CancellationToken CancellationToken;


        public TrafficWatchFileWriter(string path, CancellationToken cancellationToken)
        {
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


            _file = Path.Combine(path, _logNameCreator.GetNewFileName());
            CancellationToken = cancellationToken;
        }

        public async Task StartWritingToFile()
        {
            const int maxFileSize = 128 * 1024 * 1024;

            await using (var fileStream = new FileStream(_file, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, true))
            await using (var gZipStream = new GZipStream(fileStream, CompressionMode.Compress, false))
            await using (var fileWriter = new AsyncBlittableJsonTextWriter(_context, gZipStream))
            {
                fileWriter.WriteStartArray();
                var isFirst = true;

                try
                {
                    while (fileStream.Length < maxFileSize)
                    {
                        if (CancellationToken.IsCancellationRequested)
                        {
                            fileWriter.WriteEndArray();
                            break;
                        }

                        while (_messages.TryDequeue(out var message) &&
                               fileStream.Length < maxFileSize &&
                               CancellationToken.IsCancellationRequested == false)
                        {
                            if (isFirst == false)
                            {
                                fileWriter.WriteComma();
                            }

                            isFirst = false;

                            _context.Write(fileWriter, message.ToJson());

                            await fileWriter.FlushAsync(CancellationToken);
                        }
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    fileWriter.WriteEndArray();
                    throw;
                }
            }
        }










        // public void EnqueueMsg(TrafficWatchChangeBase msg)
        // {
        //     _messages.Enqueue(msg);
        // }




        internal class LogNameCreator
        {
            private const string LogExtension = ".log.gz";
            private const string DateFormat = "yyyy-MM-dd";
            private const string Prefix = "trafficwatch";
            private static readonly Regex LogNameRegex = new Regex($@"{Prefix}-(.{{{DateFormat.Length}}}).(\d+){LogExtension}");

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
                var match = LogNameRegex.Match(fileName);
                if (DateTime.TryParse(match.Groups[1].Value, out var dateTime) == false)
                    throw new InvalidOperationException($"Cant parse {fileName} with regex {LogNameRegex}");

                if (int.TryParse(match.Groups[2].Value, out var logNumber) == false)
                    throw new InvalidOperationException($"Cant parse {fileName} with regex {LogNameRegex}");

                return (dateTime, logNumber);
            }
        }

        public void Dispose()
        {

        }
    }
}
