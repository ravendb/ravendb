// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseNumberOfErrorIndexes : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseNumberOfErrorIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.NumberOfErrorIndexes, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(GetCount(database));
        }

        internal static int GetCount(DocumentDatabase database)
        {
            return database
                .IndexStore
                .GetIndexes()
                .Count(x => x.State == IndexState.Error);
        }
    }
}
