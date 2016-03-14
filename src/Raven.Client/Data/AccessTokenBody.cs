using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Raven.Abstractions.Data
{
    public class AccessTokenBody
    {
        public string UserId { get; set; }
        public List<ResourceAccess> AuthorizedDatabases { get; set; }
        public long Issued { get; set; }
        public bool IsServerAdminAuthorized { get; set; }

        public bool IsExpired(string tokenId, ConcurrentDictionary<string, AccessTokenBody> accessTokensById)
        {
            var ticks = Stopwatch.GetTimestamp() - Issued;
            if ((ticks*60)/Stopwatch.Frequency > 30)
            {
                AccessTokenBody removedVal;
                accessTokensById.TryRemove(tokenId, out removedVal);
                return true;
            }
            return false;
        }

        public bool IsAuthorized(string tenantId, bool writeAccess)
        {
            if (AuthorizedDatabases == null)
                return false;

            if (string.IsNullOrEmpty(tenantId) == false &&
                (tenantId.StartsWith("fs/", StringComparison.OrdinalIgnoreCase) ||
                 tenantId.StartsWith("cs/", StringComparison.OrdinalIgnoreCase) ||
                 tenantId.StartsWith("ts/", StringComparison.OrdinalIgnoreCase)))
            {
                tenantId = tenantId.Substring(3);
            }

            ResourceAccess db;
            if (string.Equals(tenantId, "<system>") || string.IsNullOrWhiteSpace(tenantId)) // TODO (OAuth): we do not have <system> anymore ..
            {
                // db = AuthorizedDatabases.FirstOrDefault(access => string.Equals(access.TenantId, "<system>"));

                return IsServerAdminAuthorized;
            }
            else
            {
                db = AuthorizedDatabases.FirstOrDefault(a =>
                                                        string.Equals(a.TenantId, tenantId, StringComparison.OrdinalIgnoreCase) ||
                                                        string.Equals(a.TenantId, "*"));
            }

            if (db == null)
                return false;

            if (db.AccessMode.Equals("Admin", StringComparison.OrdinalIgnoreCase))
                return true;

            if (db.AccessMode.Equals("Read", StringComparison.OrdinalIgnoreCase))
                return false;

            if (db.AccessMode.Equals("ReadWrite", StringComparison.OrdinalIgnoreCase) == false) // must be Admin, Read or ReadWrite
                return false;

            return true;
        }
    }
}
