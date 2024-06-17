// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseDataWrittenPerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseDataWrittenPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.DataWrittenPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.BytesPutsPerSec.OneMinuteRate;

            return (int)value;
        }
    }
}
