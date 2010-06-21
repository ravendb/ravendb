using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage.SchemaUpdates
{
    public class From24To25 : ISchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "2.4"; }
        }

        public void Update(Session session, JET_DBID dbid)
        {
            using (var tx = new Transaction(session))
            {
                using (var tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None))
                {
                    JET_COLUMNID columnid;
                    Api.JetAddColumn(session, tasks, "added_at",new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.DateTime,
                        grbit = ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);

                    using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
                    {
                        Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                        var columnids = Api.GetColumnDictionary(session, details);

                        using (var update = new Update(session, details, JET_prep.Replace))
                        {
                            Api.SetColumn(session, details, columnids["schema_version"], "2.5", Encoding.Unicode);

                            update.Save();
                        }
                    }
                }
                tx.Commit(CommitTransactionGrbit.None);
            }
        }
    }
}