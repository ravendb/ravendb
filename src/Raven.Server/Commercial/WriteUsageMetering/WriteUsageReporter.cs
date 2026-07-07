using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Commercial.WriteUsageMetering
{
    internal sealed class WriteUsageReporter : BackgroundWorkBase
    {
        private static readonly TimeSpan Interval = TimeSpan.FromMinutes(15);

        private readonly ServerStore _serverStore;
        private readonly ClusterObserver _observer;
        private readonly long _term;

        // Write-usage is only reported under a Quill license. Toggled by the LicenseChanged event so we
        // start/stop sending as the license is activated, changed, or removed.
        private volatile bool _enabled;

        public WriteUsageReporter(ServerStore serverStore, ClusterObserver observer, long term, CancellationToken token)
            : base($"Write-usage reporter for term {term}", RavenLogManager.Instance.GetLoggerForServer<WriteUsageReporter>(), token)
        {
            _serverStore = serverStore;
            _observer = observer;
            _term = term;

            UpdateEnabled();
            _serverStore.LicenseManager.LicenseChanged += OnLicenseChanged;

            Start();
        }

        protected override async Task DoWork()
        {
            // Wait first: gives the observer time to produce at least one snapshot, and spaces out reports.
            await WaitOrThrowOperationCanceled(Interval).ConfigureAwait(false);

            if (_term != _serverStore.Engine.CurrentTerm)
                return; // no longer the term this reporter was created for; the reporter will be disposed shortly.

            await ReportOnceAsync().ConfigureAwait(false);
        }

        private void OnLicenseChanged()
        {
            UpdateEnabled();
        }

        private void UpdateEnabled()
        {
            var enabled = _serverStore.LicenseManager.LicenseStatus.Type == LicenseType.Quill;

            if (enabled == _enabled)
                return;

            _enabled = enabled;

            if (Logger.IsInfoEnabled)
                Logger.Info(enabled
                    ? "Quill license detected; starting write-usage reporting to api.ravendb.net."
                    : "License is no longer Quill; stopping write-usage reporting to api.ravendb.net.");
        }

        private async Task ReportOnceAsync()
        {
            try
            {
                if (_enabled == false && _serverStore.ForTestingPurposes is not { ForceWriteUsageReportingEnabled: true })
                    return; // only report under a Quill license

                var snapshot = _observer.LatestWriteUsageSnapshot;
                if (snapshot == null)
                    return; // no maintenance tick has run yet; try again next interval

                var license = _serverStore.LoadLicense();
                if (license == null)
                    return; // no license to authenticate with; nothing to report

                // Zero-etag entries are legitimate (e.g. brand-new databases) and are reported as-is.
                var report = new WriteUsageReport(license.ToJson(), DateTime.UtcNow, snapshot.Databases);
                var body = report.ToJson();

                _serverStore.ForTestingPurposes?.OnWriteUsageReportReady?.Invoke(body);
                if (_serverStore.ForTestingPurposes is { SkipWriteUsageActualSend: true })
                    return;

                string json;
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var blittable = context.ReadObject(body, "write-usage-report"))
                {
                    json = blittable.ToString();
                }

                using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
                {
                    // no client-side retry: a failed report is simply retried on the next reporting interval.
                    var response = await ApiHttpClient
                        .PostAsync(WriteUsageMeteringConstants.WriteUsageEndpointPath, content, shouldRetry: false, token: CancellationToken)
                        .ConfigureAwait(false);

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"Reported write-usage for {report.Databases.Count} database(s) to api.ravendb.net, response: {(int)response.StatusCode} {response.StatusCode}.");
                }
            }
            catch (Exception e)
            {
                // A slow / failed / unreachable api.ravendb.net must never crash the sender or the observer.
                // log , retry on the next interval.
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to report write-usage to api.ravendb.net. Will retry on the next interval.", e);
            }
        }

        public override void Dispose()
        {
            try
            {
                _serverStore.LicenseManager.LicenseChanged -= OnLicenseChanged;
            }
            catch
            {
                // nothing actionable
            }

            base.Dispose();
        }
    }
}
