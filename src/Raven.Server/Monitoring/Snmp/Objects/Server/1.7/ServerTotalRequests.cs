// -----------------------------------------------------------------------
//  <copyright file="ServerTotalRequests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalRequests : ScalarObjectBase<Integer32>
    {
        private readonly RavenServer _server;

        public ServerTotalRequests(RavenServer server)
            : base("1.7.2")
        {
            _server = server;
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)_server.Metrics.RequestsMeter.Count);
        }
    }
}
