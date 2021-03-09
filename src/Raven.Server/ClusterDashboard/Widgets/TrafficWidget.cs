// -----------------------------------------------------------------------
//  <copyright file="TrafficWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class TrafficWidget : Widget
    {
        public override WidgetType Type => WidgetType.Traffic;

        private readonly Action<DynamicJsonValue> _onMessage;

        public TrafficWidget(int id, Action<DynamicJsonValue> onMessage, CancellationToken shutdown) : base(id, shutdown)
        {
            _onMessage = onMessage;
        }

        protected override async Task DoWork()
        {
            //TODO: 
            await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(2)); //TODO: as param
        }
    }
}
