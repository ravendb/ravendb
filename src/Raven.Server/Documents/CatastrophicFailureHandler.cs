using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class CatastrophicFailureHandler
    {
        internal TimeSpan TimeToWaitBeforeUnloadingDatabase = TimeSpan.FromSeconds(2);
        internal int MaxDatabaseUnloads = 3;
        internal TimeSpan NoFailurePeriod = TimeSpan.FromMinutes(15);

        private readonly ConcurrentDictionary<Guid, FailureStats> _errorsPerEnvironment = new ConcurrentDictionary<Guid, FailureStats>();

        private readonly DatabasesLandlord _databasesLandlord;
        private readonly ServerStore _serverStore;
        private readonly Logger _logger;
        
        public CatastrophicFailureHandler(DatabasesLandlord databasesLandlord, ServerStore serverStore)
        {
            _databasesLandlord = databasesLandlord;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<CatastrophicFailureHandler>("Server");
        }

        public bool TryGetStats(Guid environmentId, out FailureStats stats)
        {
            if (_errorsPerEnvironment.TryGetValue(environmentId, out stats))
                return true;

            return false;
        }

        public void Execute(string databaseName, Exception e, Guid environmentId, string path)
        {
            var stats = _errorsPerEnvironment.GetOrAdd(environmentId, x => FailureStats.Create(MaxDatabaseUnloads));

            if (stats.WillUnloadDatabase == false)
            {
                if (DateTime.UtcNow - stats.LastUnloadTime > NoFailurePeriod)
                {
                    // let it unload again after it was working fine for a given time with no failure

                    stats.NumberOfUnloads = 0;
                    stats.LastUnloadTime = DateTime.MinValue;
                }
                else
                {
                    return;
                }
            }

            stats.DatabaseUnloadTask = Task.Run(async () =>
            {
                var title = $"Critical error in '{databaseName}' database";
                var message = $"Database is about to be unloaded due to an encountered error in the following environment: {path}";

                try
                {
                    _serverStore.NotificationCenter.Add(AlertRaised.Create(
                        databaseName,
                        title,
                        message,
                        AlertType.CatastrophicDatabaseFailure,
                        NotificationSeverity.Error,
                        key: databaseName,
                        details: new ExceptionDetails(e)));
                }
                catch (Exception)
                {
                    // exception in raising an alert can't prevent us from unloading a database
                }

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"{title}. {message}", e);

                // let it propagate the exception to the client first and do
                // the internal failure handling e.g. Index.HandleIndexCorruption
                await Task.Delay(TimeToWaitBeforeUnloadingDatabase); 

                stats.NumberOfUnloads++;
                stats.LastUnloadTime = DateTime.UtcNow;
                (await _databasesLandlord.UnloadAndLockDatabase(databaseName, "CatastrophicFailure"))?.Dispose();
                
                stats.DatabaseUnloadTask = null;
            });
        }

        public class FailureStats
        {
            private readonly int _maxDatabaseUnloads;

            public FailureStats(int maxDatabaseUnloads)
            {
                _maxDatabaseUnloads = maxDatabaseUnloads;
            }

            public int NumberOfUnloads;
            public DateTime? LastUnloadTime;
            public Task DatabaseUnloadTask;

            public bool WillUnloadDatabase => NumberOfUnloads < _maxDatabaseUnloads;

            public static FailureStats Create(int maxDatabaseUnloads)
            {
                return new FailureStats(maxDatabaseUnloads);
            }
        }
    }
}
