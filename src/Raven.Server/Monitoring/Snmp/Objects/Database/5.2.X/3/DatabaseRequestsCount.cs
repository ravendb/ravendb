// -----------------------------------------------------------------------
//  <copyright file="DatabaseRequestsPerSecond.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseRequestsCount : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseRequestsCount(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.RequestsCount, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.Requests.RequestsPerSec.Count;
        }
    }
}
