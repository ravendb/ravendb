// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseNumberOfAutoIndexes : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseNumberOfAutoIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.5.3", index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var count = database
                .IndexStore
                .GetIndexes()
                .Count(x => x.Type.IsAuto());

            return new Integer32(count);
        }
    }
}
