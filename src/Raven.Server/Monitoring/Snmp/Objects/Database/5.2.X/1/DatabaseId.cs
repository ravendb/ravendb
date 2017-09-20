// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseId : DatabaseScalarObjectBase<OctetString>
    {
        private OctetString _id;

        public DatabaseId(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.1.11", index)
        {

        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            return _id ?? (_id = new OctetString(database.DocumentsStorage.Environment.DbId.ToString()));
        }
    }
}
