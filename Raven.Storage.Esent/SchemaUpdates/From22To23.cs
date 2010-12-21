//-----------------------------------------------------------------------
// <copyright file="From22To23.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
    public class From22To23 : ISchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "2.2"; }
        }
        private IUuidGenerator uuidGenerator;

        public void Init(IUuidGenerator generator)
        {
            uuidGenerator = generator;
        }

        public void Update(Session session, JET_DBID dbid)
        {
            using (var tx = new Transaction(session))
            {

                new SchemaCreator(session).CreateQueueTable(dbid);

                using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
                {
                    Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                    var columnids = Api.GetColumnDictionary(session, details);

                    using (var update = new Update(session, details, JET_prep.Replace))
                    {
                        Api.SetColumn(session, details, columnids["schema_version"], "2.3", Encoding.Unicode);

                        update.Save();
                    }
                }
                tx.Commit(CommitTransactionGrbit.None);
            }
        }
    }
}
