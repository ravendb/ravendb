using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Plugins;
using Raven.Database.Server;


namespace Raven.Database.Config
{
    public class SystemInfoLoggin : IServerStartupTask
    {
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();

        private RavenDBOptions options;
        private Timer timer;

        private readonly StringBuilder logText = new StringBuilder();

        public void Execute(RavenDBOptions serverOptions)
        {
            options = serverOptions;
            timer = options.SystemDatabase.TimerManager.NewTimer(ExecuteCheck, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(2));
        }

        private void ExecuteCheck(object state)
        {
            try
            {
                if (options.Disposed)
                {
                    Dispose();
                    return;
                }

                if (Log.IsDebugEnabled == false)
                    return;

                logText.Clear();

                logText.AppendFormat("Version: {0} / {1}\r\n", DocumentDatabase.BuildVersion, DocumentDatabase.ProductVersion);

                logText.AppendFormat("Total Mem: {0:#,#}MB, Available: {1:#,#}MB, Mem limit: {2:#,#}MB, Low mem: {3}\r\n",
                    MemoryStatistics.TotalPhysicalMemory,
                    MemoryStatistics.AvailableMemoryInMb,
                    MemoryStatistics.MemoryLimit,
                    MemoryStatistics.IsLowMemory);

                options.DatabaseLandlord.ForAllDatabases(database =>
                {
                    logText.AppendFormat("DebugInfo for Database '{0}' :\r\n= = = = = = = = = = = = = = = = \r\n", database.Name);

                    LogMetrics(database);
                    LogVoronStats(database);
                    LogEtags(database);
                    // TODO : LogIndexes (stale etc.. ?)
                });

                options.FileSystemLandlord.ForAllFileSystems(filesystem =>
                {
                    // TODO : Log FS as well
                });

                WriteCurrentServerStateInformationToLog();
            }
            catch (Exception e)
            {
                Log.WarnException("Error when generating log metrics, log metrics will be disabled", e);
                try
                {
                    Dispose();
                }
                catch (Exception)
                {
                    
                }
            }
        }

        private void WriteCurrentServerStateInformationToLog()
        {
            if (Log.IsDebugEnabled)
                Log.Debug(logText.ToString());
            logText.Clear();
        }

        private void LogMetrics(DocumentDatabase database)
        {

            var metrics = database.CreateMetrics();

            logText.AppendFormat("\tMetrics:: DocsWritesPerSecond: {0:N3}, IndexedPerSecond: {1:N3}, ReducedPerSecond: {2:N3}, RequestsPerSecond: {3:N3}\r\n",
                metrics.DocsWritesPerSecond,
                metrics.IndexedPerSecond,
                metrics.ReducedPerSecond,
                metrics.RequestsPerSecond);

            logText.AppendFormat("\tMetrics.Requests:: Count: {0:#,#}, OneMinuteRate: {1:N3}, MeanRate: {2:N3}\r\n",
                metrics.Requests.Count,
                metrics.Requests.OneMinuteRate,
                metrics.Requests.MeanRate);

            logText.AppendFormat("\tMetrics.RequestsDuration:: Counter: {0:#,#}, Max: {1:N3}, Min: {2:N3}, Mean: {3:N3}, Stdev: {4:N3}\r\n",
                metrics.RequestsDuration.Counter,
                metrics.RequestsDuration.Max,
                metrics.RequestsDuration.Min,
                metrics.RequestsDuration.Mean,
                metrics.RequestsDuration.Stdev);

            logText.AppendFormat("\tMetrics.StaleIndexMaps:: Counter: {0:#,#}, Max: {1:N3}, Min: {2:N3}, Mean: {3:N3}, Stdev: {4:N3}\r\n",
                metrics.StaleIndexMaps.Counter,
                metrics.StaleIndexMaps.Max,
                metrics.StaleIndexMaps.Min,
                metrics.StaleIndexMaps.Mean,
                metrics.StaleIndexMaps.Stdev);

            logText.AppendFormat("\tMetrics.StaleIndexMaps:: Counter: {0:#,#}, Max: {1:N3}, Min: {2:N3}, Mean: {3:N3}, Stdev: {4:N3}\r\n",
                metrics.StaleIndexReduces.Counter,
                metrics.StaleIndexReduces.Max,
                metrics.StaleIndexReduces.Min,
                metrics.StaleIndexReduces.Mean,
                metrics.StaleIndexReduces.Stdev);
        }

        private void LogVoronStats(DocumentDatabase database)
        {
            if (database.TransactionalStorage is Raven.Storage.Voron.TransactionalStorage == false)
                return;

            var voronStats = database.TransactionalStorage.GetStorageStats().VoronStats;

            if (voronStats == null)
                return;

            logText.AppendFormat("VoronStats:: FreePagesOverhead: {0:#,#}, RootPages: {1:#,#}, FreePagesOverhead: {2:#,#}, UsedDataFileSizeInBytes: {3:#,#}, AllocatedDataFileSizeInBytes: {4:#,#}, NextWriteTransactionId: {5:#,#}\r\n",
                voronStats.FreePagesOverhead,
                voronStats.RootPages,
                voronStats.UnallocatedPagesAtEndOfFile,
                voronStats.UsedDataFileSizeInBytes,
                voronStats.AllocatedDataFileSizeInBytes,
                voronStats.NextWriteTransactionId);
        }

        private class EtagsAndIndexes
        {
            public Etag Etag;
            public string IndexName;
        }

        private class GroupedEtagsAndIndexes
        {
            public Etag Etag;
            public List<string> IndexNamesList { get; set; }
        }

        private void LogEtags(DocumentDatabase database)
        {
            // indexes etags:
            var etagsList = new List<EtagsAndIndexes>();
            var indexes = database.IndexStorage.Indexes;

            foreach (var i in indexes)
            {
                var indexInstance = database.IndexStorage.GetIndexInstance(i);
                var etag = database.IndexStorage.GetLastEtagForIndex(indexInstance) ?? Etag.Empty;
                etagsList.Add(new EtagsAndIndexes()
                {
                    Etag = etag,
                    IndexName = indexInstance.PublicName
                });
            }

            var groupedEtagsAndIndexes = etagsList
                .GroupBy(x => x.Etag)
                .Select(y => new GroupedEtagsAndIndexes()
                {
                    Etag = y.Key,
                    IndexNamesList = y.Select(x => x.IndexName).ToList()
                })
                .Distinct();

            foreach (var i in groupedEtagsAndIndexes)
            {
                logText.AppendFormat("Etag={0} Indexes={{", i.Etag);
                foreach (var j in i.IndexNamesList)
                {
                    logText.AppendFormat("{0},", j);
                }
                if (i.IndexNamesList.Count > 0)
                    logText.Length--;
                logText.AppendFormat("}}\r\n");
            }

            // last doc etag:
            var lastDocEtag = Etag.Empty;
            long documentsCount = 0;
            database.TransactionalStorage.Batch(
                accessor =>
                {
                    lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                    documentsCount = accessor.Documents.GetDocumentsCount();
                });

            lastDocEtag = lastDocEtag.HashWith(BitConverter.GetBytes(documentsCount));
            logText.AppendFormat("Last DB Etag={0}\r\n", lastDocEtag);
        }

        public void Dispose()
        {
            var copy = timer;
            if (copy != null)
            {
                copy.Dispose();
                timer = null;
            }
        }
    }
}

