using System;
using System.Threading;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class PgWatcher
    {
        private readonly RavenServer _server;
        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);
        private PgServer _pgServer;

        public PgWatcher(RavenServer server)
        {
            _server = server;

            _server.ServerStore.LicenseManager.LicenseChanged += OnLicenseChanged;
        }
        
        private void OnLicenseChanged()
        {
            if (_server.Configuration.Integrations.PostgreSQL.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                var pgServer = _pgServer;
                if (pgServer == null)
                    return;

                var activate = _server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true);
                if (activate)
                {
                    if (pgServer.Active == false)
                        pgServer.Start();
                }
                else
                {
                    pgServer.Stop();
                }
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                _locker.Release();
            }
        }

        public void Execute()
        {
            if (_server.Configuration.Integrations.PostgreSQL.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                _pgServer = new PgServer(_server);

                var activate = _server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true);
                if (activate)
                {
                    if (_pgServer.Active)
                        throw new InvalidOperationException("Cannot start PgServer because it is already activated. Should not happen!");

                    _pgServer.Start();
                }
            }
            finally
            {
                _locker.Release();
            }
        }
    }
}
