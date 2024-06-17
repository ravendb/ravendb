using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class MetricsManager
{
    private readonly RavenServer _server;
    private readonly SemaphoreSlim _locker = new(1, 1);
    private ServerMetrics _serverMetrics;
    
    public MetricsManager(RavenServer server)
    {
        _server = server;
        _server.ServerStore.LicenseManager.LicenseChanged += OnLicenseChanged;
    }

    private void OnLicenseChanged()
    {
        if (_server.Configuration.Monitoring.OpenTelemetry.Enabled == false)
            return;

        _locker.Wait();
        try
        {
            var activate = _server.ServerStore.LicenseManager.CanUseOpenTelemetryMonitoring(withNotification: true, startUp: false);
            if (activate)
            {
                Execute();
            }
        }
        catch (ObjectDisposedException)
        {
            // ignore
            // we are shutting down the server
        }
        finally
        {
            _locker.Release();
        }
    }

    public void Execute()
    {
        if (_server.Configuration.Monitoring.OpenTelemetry.Enabled == false)
            return;

        _locker.Wait();

        try
        {
            var activate = _server.ServerStore.LicenseManager.CanUseOpenTelemetryMonitoring(withNotification: true, startUp: true);
            if (activate)
            {
                if (_serverMetrics != null)
                    throw new InvalidOperationException("Cannot start SNMP Engine because it is already activated. Should not happen!");

                RegisterServerWideMeters();
            }
        }
        finally
        {
            _locker.Release();
        }
    }

    private void RegisterServerWideMeters()
    {
        if (_serverMetrics != null)
            throw new InvalidOperationException("Server-wide metrics are already initialized.");

        if (_server.Configuration.Monitoring.OpenTelemetry.ServerWideEnabled == false)
            return;

        _serverMetrics = new(_server);
    }
}
