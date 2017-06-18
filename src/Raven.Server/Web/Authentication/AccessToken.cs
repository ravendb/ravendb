using System;
using System.Collections.Generic;
using Raven.Client.Server.Operations.ApiKeys;

namespace Raven.Server.Web.Authentication
{
    public class AccessToken
    {
        public string Name { get; set; }
        public string NodeTag { get; set; }
        public string Token { get; set; }
        public DateTime Expires { get; set; }

        public Dictionary<string, AccessModes> AuthorizedDatabases { get; set; }

        public bool IsExpired => Expires.CompareTo(DateTime.UtcNow) <= 0;
    }
}
