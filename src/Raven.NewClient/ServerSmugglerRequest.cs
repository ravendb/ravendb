// -----------------------------------------------------------------------
//  <copyright file="ServerSmugglerRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;

namespace Raven.NewClient.Abstractions
{
    public class ServerSmugglerRequest
    {
        public ServerConnectionInfo TargetServer { get; set; }
        public List<ServerSmugglingItem> Config { get; set; }
    }

    public class ServerConnectionInfo
    {
        public string Url { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Domain { get; set; }
        public string ApiKey { get; set; }

        public ICredentials Credentials
        {
            get
            {
                return string.IsNullOrEmpty(Username) == false ? new NetworkCredential(Username, Password, Domain ?? string.Empty) : null;
            }
        }
    }

    public class ServerSmugglingItem
    {
        public string Name { get; set; }
        public bool Incremental { get; set; }
        public bool StripReplicationInformation { get; set; }
        public bool ShouldDisableVersioningBundle { get; set; }
    }

}
