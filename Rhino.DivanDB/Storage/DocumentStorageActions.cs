using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web.Caching;
using log4net;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.DivanDB.Storage
{
    [CLSCompliant(false)]
    public class DocumentStorageActions : IDisposable
    {
        private ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));

        private readonly Session session;
        private readonly Guid instanceId;
        private readonly JET_DBID dbid;
        private readonly Transaction transaction;
        private readonly Table documents;
        private readonly IDictionary<string, JET_COLUMNID> documentsColumns;


        public DocumentStorageActions(JET_INSTANCE instance,
                                          string database,
                                          Guid instanceId)
        {
            this.instanceId = instanceId;
            session = new Session(instance);
            transaction = new Transaction(session);
            Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

            documents = new Table(session, dbid, "documents", OpenTableGrbit.None);
            documentsColumns = Api.GetColumnDictionary(session, documents);
        }

        public void AddDocument(string key, string data)
        {
            using (var update = new Update(session, documents, JET_prep.Insert))
            {
                Api.SetColumn(session, documents, documentsColumns["key"], key, Encoding.Unicode);
                Api.SetColumn(session, documents, documentsColumns["data"], data, Encoding.Unicode);

                update.Save();
            }
            logger.DebugFormat("Inserted a new document with key '{0}', doc length: {1}", key, data.Length);
        }

        public string DocumentByKey(string key)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if(Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found", key);
                return null;
            }
            var data = Api.RetrieveColumnAsString(session, documents, documentsColumns["data"]);
            logger.DebugFormat("Document with key '{0}' was found, doc lenght: {1}", key, data.Length);
            return data;
        }

        public void Dispose()
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

        public void DeleteDocument(string key)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
            }

            Api.JetDelete(session, documents);
            logger.DebugFormat("Document with key '{0}' was deleted", key);
        }
    }
}