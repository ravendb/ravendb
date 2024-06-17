// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseNumberOfIndexes : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseNumberOfIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.NumberOfIndexes, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32((int)database.IndexStore.Count);
        }
    }
}
