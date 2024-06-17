// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexAttempts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexErrors : DatabaseIndexScalarObjectBase<Integer32>
    {
        public DatabaseIndexErrors(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Errors)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Integer32((int)index.GetErrorCount());
        }
    }
}
