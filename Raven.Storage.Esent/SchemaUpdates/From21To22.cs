using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Indexing;

namespace Raven.Storage.Esent.SchemaUpdates
{
    public class From21To22 : ISchemaUpdate
    {
        public string FromSchemaVersion
        {
            get { return "2.1"; }
        }

        private IUuidGenerator uuidGenerator;

        public void Init(IUuidGenerator generator)
        {
            uuidGenerator = generator;
        }

        public void Update(Session session, JET_DBID dbid)
        {
            using(var tx = new Transaction(session))
            {
                using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
                {
                    JET_COLUMNID columnid;
                    Api.JetAddColumn(session, mappedResults, "reduce_key_and_view_hashed", new JET_COLUMNDEF
                    {
                        cbMax = 32,
                        coltyp = JET_coltyp.Binary,
                        grbit = ColumndefGrbit.ColumnNotNULL
                    }, null, 0, out columnid);

                    const string indexDef = "+reduce_key_and_view_hashed\0\0";
                    Api.JetCreateIndex(session, mappedResults, "by_reduce_key_and_view_hashed",
                                       CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                                       100);

                    Api.JetDeleteIndex(session, mappedResults, "by_view_and_reduce_key");


                    var columnDictionary = Api.GetColumnDictionary(session, mappedResults);

                    Api.MoveBeforeFirst(session, mappedResults);
                    while (Api.TryMoveNext(session, mappedResults))
                    {
                        using (var update = new Update(session, mappedResults, JET_prep.Replace))
                        {
                            var computeHash = MapReduceIndex.ComputeHash(
                                Api.RetrieveColumnAsString(session, mappedResults, columnDictionary["view"],
                                                           Encoding.Unicode),
                                Api.RetrieveColumnAsString(session, mappedResults, columnDictionary["reduce_key"],
                                                           Encoding.Unicode));

                            Api.SetColumn(session, mappedResults, columnDictionary["reduce_key_and_view_hashed"],
                                          computeHash);

                            update.Save();
                        }
                    }


                    using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
                    {
                        Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                        var columnids = Api.GetColumnDictionary(session, details);

                        using(var update = new Update(session, details, JET_prep.Replace))
                        {
                            Api.SetColumn(session, details, columnids["schema_version"], "2.2", Encoding.Unicode);

                            update.Save();
                        }
                    }
                }
                tx.Commit(CommitTransactionGrbit.None);
            }
        }
    }
}
