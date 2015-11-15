//-----------------------------------------------------------------------
// <copyright file="EsentExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent
{
    [CLSCompliant(false)]
    public static class EsentExtension
    {
        public static void WithDatabase(this JET_INSTANCE instance, string database, Func<Session, JET_DBID, Transaction, Transaction> action)
        {
            using (var session = new Session(instance))
            {
                var tx = new Transaction(session);
                try
                {
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, database, "", out dbid, OpenDatabaseGrbit.None);
                    try
                    {
                        tx = action(session, dbid, tx);
                    }
                    finally
                    {
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    }
                    tx.Commit(CommitTransactionGrbit.None);
                }
                finally
                {
                    if(tx != null)
                        tx.Dispose();
                }
            }
        }
    }
}
