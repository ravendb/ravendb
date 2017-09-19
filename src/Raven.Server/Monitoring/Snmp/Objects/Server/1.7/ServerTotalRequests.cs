// -----------------------------------------------------------------------
//  <copyright file="ServerTotalRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalRequests : ScalarObjectBase<Integer32>
    {
        private readonly MetricsCountersManager _metrics;

        public ServerTotalRequests(MetricsCountersManager metrics)
            : base("1.7.2")
        {
            _metrics = metrics;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_metrics.RequestsMeter.Count);
        }
    }
}
