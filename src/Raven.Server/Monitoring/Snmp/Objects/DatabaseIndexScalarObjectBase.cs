// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexScalarObjectBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Monitoring.Snmp.Objects
{
    public abstract class DatabaseIndexScalarObjectBase<TData> : DatabaseScalarObjectBase<TData>
        where TData : ISnmpData
    {
        protected readonly string IndexName;

        protected DatabaseIndexScalarObjectBase(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string dots)
            : base(databaseName, landlord, string.Format("5.2.{0}.4.{{0}}.{1}", databaseIndex, dots), indexIndex)
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
                        return DefaultValue();

                    return GetData(database);
                }

                return DefaultValue();
            }
        }

        protected Index GetIndex(DocumentDatabase database)
        {
            return database.IndexStore.GetIndex(IndexName);
        }
    }
}
