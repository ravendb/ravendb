using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Client.Data
{
    public class AccessToken
    {
        public enum Mode
        {
            None,
            ReadOnly,
            ReadWrite,
            Admin
        }
        public string Token { get; set; }
        public string Name { get; set; }
        public Dictionary<string, Mode> AuthorizedDatabases { get; set; }
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
