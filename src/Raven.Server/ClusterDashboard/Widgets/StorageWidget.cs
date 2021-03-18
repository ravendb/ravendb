// -----------------------------------------------------------------------
//  <copyright file="StorageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class StorageWidget : Widget
    {
        private readonly RavenServer _server;
        private readonly Action<StoragePayload> _onMessage;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromMinutes(1);

        public StorageWidget(int id, RavenServer server, Action<StoragePayload> onMessage, CancellationToken shutdown) : base(id, shutdown)
        {
            _server = server;
            _onMessage = onMessage;
        }

        public override WidgetType Type => WidgetType.Storage;
        
        protected override async Task DoWork()
        {
            // TODO: I'm empty for now.... Please implement me

            await WaitOrThrowOperationCanceled(_defaultInterval);
        }
    }
}
