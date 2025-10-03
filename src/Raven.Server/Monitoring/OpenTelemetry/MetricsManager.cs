using System;
using System.Threading;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class MetricsManager
{
    private readonly RavenServer _server;
    private readonly bool _metersRegisteredOnInitialize;
    private readonly SemaphoreSlim _locker = new(1, 1);
    private ServerMetrics _serverMetrics;
    private bool _metricsActivated;
    
    public MetricsManager(RavenServer server, bool metersRegisteredOnInitialize)
    {
        _server = server;
        _metersRegisteredOnInitialize = metersRegisteredOnInitialize;
        _server.ServerStore.LicenseManager.LicenseChanged += OnLicenseChanged;
    }

    private void OnLicenseChanged()
    {
        try
        {
            Execute();
        }
        catch (ObjectDisposedException)
        {
            // ignore
            // we are shutting down the server
        }
    }

    public void Execute()
    {
        if (_server.Configuration.Monitoring.OpenTelemetry.Enabled == false)
            return;

        _locker.Wait();

        try
        {
            if (_metricsActivated)
                return;
            
            var activate = _server.ServerStore.LicenseManager.CanUseOpenTelemetryMonitoring(withNotification: true, metersRegistered: _metersRegisteredOnInitialize);
            if (activate)
            {
                if (_serverMetrics != null)
                    throw new InvalidOperationException("Cannot start OpenTelemetry because it is already activated. Should not happen!");

                RegisterServerMeters();
            }
        }
        finally
        {
            _locker.Release();
        }
    }

    private void RegisterServerMeters()
    {
        if (_serverMetrics != null)
            throw new InvalidOperationException("Server-wide metrics are already initialized.");

        if (_server.Configuration.Monitoring.OpenTelemetry.ServerMetersEnabled == false)
            return;

        _serverMetrics = new(_server);
        _metricsActivated = true;
    }
}
