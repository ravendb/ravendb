using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.SchemaUpdates
{
    public class From22To23 : ISchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "2.2"; }
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