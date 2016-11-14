using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Client.Data
{
    public class AccessToken
    {
        public string Token { get; set; }
        public string Name { get; set; }
        public Dictionary<string, AccessModes> AuthorizedDatabases { get; set; }
        public long Issued { get; set; }
        public bool IsServerAdminAuthorized { get; set; }

        public static readonly long MaxAge = Stopwatch.Frequency * 60 * 30;// 30 minutes

        public bool IsExpired
        {
            get
            {
                var ticks = Stopwatch.GetTimestamp() - Issued;
                return ticks > MaxAge;
            }
        }
    }
}
