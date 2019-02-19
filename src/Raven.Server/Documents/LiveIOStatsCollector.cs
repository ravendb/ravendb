using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Sparrow.Extensions;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Server.Meters;
using Sparrow.Utils;

namespace Raven.Server.Documents
{
    public class LiveIOStatsCollector : IDisposable
    {
        // Dictionary to hold the ioFileItems written by server  
        private readonly ConcurrentDictionary<string, BlockingCollection<IoMeterBuffer.MeterItem>> _perEnvironmentsFilesMetrics; // Path+fileName is the key
        // Queue for Endpoint to read from
        public AsyncQueue<IOMetricsResponse> MetricsQueue { get; } = new AsyncQueue<IOMetricsResponse>();

        private string _basePath;
        private readonly CancellationToken _resourceShutdown;
        private readonly CancellationTokenSource _cts;
        private readonly DocumentDatabase _documentDatabase;

        public LiveIOStatsCollector(DocumentDatabase documentDatabase)
        {
            _resourceShutdown = documentDatabase.DatabaseShutdown; // cancellation token
            _documentDatabase = documentDatabase;
            _perEnvironmentsFilesMetrics = new ConcurrentDictionary<string, BlockingCollection<IoMeterBuffer.MeterItem>>();
            _cts = new CancellationTokenSource();

            Task.Run(StartCollectingMetrics);
        }

        public void Dispose()
        {
            _documentDatabase.IoChanges.OnIoChange -= OnIOChange;
            _cts.Cancel();
            _cts.Dispose();
        }

        private async Task StartCollectingMetrics()
        {
            _documentDatabase.IoChanges.OnIoChange += OnIOChange;

            // 1. First time around, get existing data
            var result = IoMetricsHandler.GetIoMetricsResponse(_documentDatabase);

            _basePath = result.Environments[0].Path;
            result.Environments[0].Path = Path.Combine(_basePath, "Documents");

            foreach (var environment in result.Environments)
            {
                var folder = environment.Path;
                foreach (var file in environment.Files)
                {
                    var fullFilePath = Path.Combine(folder, file.File);
                    _perEnvironmentsFilesMetrics.TryAdd(fullFilePath, new BlockingCollection<IoMeterBuffer.MeterItem>());
                }
            }

            MetricsQueue.Enqueue(result);

            using (var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(_resourceShutdown, _cts.Token))
            {
                var token = linkedToken.Token;

                // 2. Prepare & put data from the Dictionary into the Queue every 3 seconds
                while (token.IsCancellationRequested == false)
                {
                    await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000), token).ConfigureAwait(false);

                    if (token.IsCancellationRequested)
                        break;

                    var ioMetricsResponse = PrepareIOMetrics();
                    if (ioMetricsResponse != null)
                    {
                        MetricsQueue.Enqueue(ioMetricsResponse);
                    }
                }
            }
        }

        private IOMetricsResponse PrepareIOMetrics()
        {
            // 0. Prepare Response Object - Keep envs order the same as in the static endpoint response !
            var preparedMetricsResponse = new IOMetricsResponse();
            bool responseHasContent = false;

            // 1. Iterate over environments files in dictionary
            foreach (var envFile in _perEnvironmentsFilesMetrics)
            {
                // 2. Retrieve/Take meter items per environment file from the collection in dictionary
                var listOfMeterItems = new List<IoMeterBuffer.MeterItem>();
                while (envFile.Value.TryTake(out IoMeterBuffer.MeterItem newItem))
                {
                    listOfMeterItems.Add(newItem);
                }

                if (listOfMeterItems.Count == 0)
                    continue;

                // 3. Get env path & file name from dictionary item
                var meterItem = listOfMeterItems[0];
                var file = new FileInfo(envFile.Key);
                var envPath = file.Directory;
                if (meterItem.Type == IoMetrics.MeterType.Compression || meterItem.Type == IoMetrics.MeterType.JournalWrite)
                    envPath = envPath?.Parent;

                // 3a. Should not happen, but being extra careful here
                if (envPath == null)
                    continue;

                // 4. Find relevant environment
                var currentEnvironment = preparedMetricsResponse.Environments.FirstOrDefault(x => x.Path == envPath.FullName);
                if (currentEnvironment == null)
                {
                    // If new index for example was added...
                    currentEnvironment = new IOMetricsEnvironment { Path = envPath.FullName, Files = new List<IOMetricsFileStats>() };
                    preparedMetricsResponse.Environments.Add(currentEnvironment);
                }

                if (currentEnvironment.Path == _basePath)
                {
                    currentEnvironment.Path = Path.Combine(_basePath, "Documents");
                }

                // 5. Prepare response, add recent items.  Note: History items are not added since studio does not display them anyway
                var preparedFilesInfo = currentEnvironment.Files.FirstOrDefault(x => x.File == file.Name) ?? new IOMetricsFileStats
                {
                    File = file.Name
                };

                currentEnvironment.Files.Add(preparedFilesInfo);

                foreach (var item in listOfMeterItems)
                {
                    var preparedRecentStats = new IOMetricsRecentStats
                    {
                        Start = item.Start.GetDefaultRavenFormat(true),
                        Size = item.Size,
                        HumaneSize = Sizes.Humane(item.Size),
                        FileSize = item.FileSize,
                        HumaneFileSize = Sizes.Humane(item.FileSize),
                        Duration = Math.Round(item.Duration.TotalMilliseconds, 2),
                        Type = item.Type
                    };

                    responseHasContent = true;
                    preparedFilesInfo.Recent.Add(preparedRecentStats);
                }
            }

            if (responseHasContent == false)
                return null;

            return preparedMetricsResponse;
        }

        private void OnIOChange(IoChange recentFileIoItem)
        {
            // recentFileIoItem is the recent item that was written to disk by the server
            // Add the new item to  the File items ConcurrentDictionary collection
            var collection = _perEnvironmentsFilesMetrics.GetOrAdd(recentFileIoItem.FileName, new BlockingCollection<IoMeterBuffer.MeterItem>());
            collection.Add(recentFileIoItem.MeterItem, CancellationToken.None);
        }
    }
}
