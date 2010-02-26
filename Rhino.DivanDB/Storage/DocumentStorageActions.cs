using System;
using System.Collections.Generic;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.DivanDB.Storage
{
    [CLSCompliant(false)]
    public class DocumentStorageActions : IDisposable
    {
        protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));

        protected readonly Session session;
        protected readonly JET_DBID dbid;
        private readonly Transaction transaction;
        protected readonly Table documents;
        protected readonly IDictionary<string, JET_COLUMNID> documentsColumns;


        public DocumentStorageActions(JET_INSTANCE instance,
                                          string database)
        {
            try
            {
                session = new Session(instance);
                transaction = new Transaction(session);
                Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

                documents = new Table(session, dbid, "documents", OpenTableGrbit.None);
                documentsColumns = Api.GetColumnDictionary(session, documents);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public string DocumentByKey(string key)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found", key);
                return null;
            }
            var data = Api.RetrieveColumnAsString(session, documents, documentsColumns["data"]);
            logger.DebugFormat("Document with key '{0}' was found, doc length: {1}", key, data.Length);
            return data;
        }

        public virtual void Dispose()
        {
            if (documents != null)
                documents.Dispose();

            if (Equals(dbid, JET_DBID.Nil) == false)
                Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);

            if (transaction != null)
                transaction.Dispose();

            if (session != null)
                session.Dispose();
        }

        public void Commit()
        {
            transaction.Commit(CommitTransactionGrbit.None);
        }

        public IEnumerable<string> DocumentKeys
        {
            get
            {
                Api.MoveBeforeFirst(session, documents);
                while (Api.TryMoveNext(session, documents))
                {
                    yield return Api.RetrieveColumnAsString(session, documents, documentsColumns["key"], Encoding.Unicode);
                }
            }
        }


        public FirstAndLast FirstAndLastDocumentKeys()
        {
            var result = new FirstAndLast();
            Api.MoveBeforeFirst(session, documents);
            if (Api.TryMoveNext(session, documents))
                result.First = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"]).Value;
            Api.MoveAfterLast(session, documents);
            if (Api.TryMovePrevious(session, documents))
                result.Last = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"]).Value;
            return result;
        }
    }
}