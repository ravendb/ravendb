// -----------------------------------------------------------------------
//  <copyright file="LicenseWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class LicenseWidget : Widget
    {
        private readonly RavenServer _server;
        private readonly Action<LicensePayload> _onMessage;
        
        private readonly TimeSpan _defaultInterval = TimeSpan.FromHours(1);

        public LicenseWidget(int id, RavenServer server, Action<LicensePayload> onMessage, CancellationToken shutdown) : base(id, shutdown)
        {
            _server = server;
            _onMessage = onMessage;
        }

        public override WidgetType Type => WidgetType.License;

        protected override async Task DoWork()
        {
            // TODO: I'm empty for now.... Please implement me

            await WaitOrThrowOperationCanceled(_defaultInterval);
        }
    }
}
