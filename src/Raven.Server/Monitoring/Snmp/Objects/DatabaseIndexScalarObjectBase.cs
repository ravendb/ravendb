// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Monitoring.Snmp.Objects
{
    public abstract class DatabaseIndexScalarObjectBase<TData> : DatabaseScalarObjectBase<TData>
        where TData : ISnmpData
    {
        protected readonly string IndexName;

        protected DatabaseIndexScalarObjectBase(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string dots)
            : base(databaseName, landlord, string.Format(dots, databaseIndex), indexIndex)
        {
            IndexName = indexName;
        }

        public override ISnmpData Data
        {
            get
            {
                if (Landlord.IsDatabaseLoaded(DatabaseName))
                {
                    var database = Landlord.TryGetOrCreateResourceStore(DatabaseName).Result;
                    var index = GetIndex(database);
                    if (index == null)
                        return null;

                    return GetData(database);
                }

                return null;
            }
        }

        protected Index GetIndex(DocumentDatabase database)
        {
            return database.IndexStore.GetIndex(IndexName);
        }
    }
}
