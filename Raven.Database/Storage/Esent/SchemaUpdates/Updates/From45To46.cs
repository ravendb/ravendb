﻿// -----------------------------------------------------------------------
//  <copyright file="From45To46.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Impl;
using BitConverter = System.BitConverter;

namespace Raven.Storage.Esent.SchemaUpdates.Updates
{
    public class From45To46 : ISchemaUpdate
    {
        public string FromSchemaVersion { get { return "4.5"; } }
        
        public void Init(IUuidGenerator generator)
        {
        }

        public void Update(Session session, JET_DBID dbid)
        {
            using (var table = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
            {
                byte[] defaultValue = BitConverter.GetBytes(0);
                JET_COLUMNID columnid;
                Api.JetAddColumn(session, table, "priority", new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Long,
                    grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
                }, defaultValue, defaultValue.Length, out columnid);
            }

            SchemaCreator.UpdateVersion(session,   dbid, "4.6");
        }
    }
}