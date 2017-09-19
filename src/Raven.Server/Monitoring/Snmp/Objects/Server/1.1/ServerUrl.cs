// -----------------------------------------------------------------------
//  <copyright file="ServerName.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Config;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerUrl : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _url;

        public ServerUrl(RavenConfiguration configuration)
            : base("1.1.1")
        {
            _url = new OctetString(configuration.Core.ServerUrl);
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
