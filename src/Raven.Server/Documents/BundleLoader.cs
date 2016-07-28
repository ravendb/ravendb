using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Documents.Versioning;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class BundleLoader : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        public VersioningStorage VersioningStorage;
        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner;
        public PeriodicExportRunner PeriodicExportRunner;

        public BundleLoader(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            _logger = _database.LoggerSetup.GetLogger<BundleLoader>(_database.Name);
        }

        public void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            var key = notification.Key;
            if (key.Equals(Constants.Versioning.RavenVersioningConfiguration, StringComparison.OrdinalIgnoreCase))
            {
                VersioningStorage = VersioningStorage.LoadConfigurations(_database);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Versioning configuration was {(VersioningStorage  != null ? "disabled" : "enabled")}");
            }
            else if(key.Equals(Constants.Expiration.ConfigurationDocumentKey, StringComparison.OrdinalIgnoreCase))
            {
                ExpiredDocumentsCleaner?.Dispose();
                ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(_database);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Expiration configuration was {(ExpiredDocumentsCleaner != null ? "enabled" : "disabled")}");
            }
            else if (key.Equals(Constants.PeriodicExport.ConfigurationDocumentKey, StringComparison.OrdinalIgnoreCase))
            {
                PeriodicExportRunner?.Dispose();
                PeriodicExportRunner = PeriodicExportRunner.LoadConfigurations(_database);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Expiration configuration was {(ExpiredDocumentsCleaner != null ? "enabled" : "disabled")}");
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(BundleLoader)}");
            exceptionAggregator.Execute(() =>
            {
                ExpiredDocumentsCleaner?.Dispose();
                ExpiredDocumentsCleaner = null;
            });
            exceptionAggregator.Execute(() =>
            {
                PeriodicExportRunner?.Dispose();
                PeriodicExportRunner = null;
            });
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}