using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Rhino.DivanDB.Tasks;

namespace Rhino.DivanDB.Storage
{
    [CLSCompliant(false)]
    public class DocumentStorageActions : IDisposable
    {
        protected readonly ILog logger = LogManager.GetLogger(typeof(DocumentStorageActions));
        private int innerTxCount;

        protected readonly Session session;
        protected readonly JET_DBID dbid;
        private readonly Transaction transaction;
        protected readonly Table documents;
        protected readonly IDictionary<string, JET_COLUMNID> documentsColumns;
        protected readonly Table tasks;
        protected readonly IDictionary<string, JET_COLUMNID> tasksColumns;

        public bool CommitCalled { get; set; }

        [CLSCompliant(false)]
        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
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
                tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None);
                tasksColumns = Api.GetColumnDictionary(session, tasks);
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

        public void Dispose()
        {
            if (documents != null)
                documents.Dispose();

            if (tasks != null)
                tasks.Dispose();

            if (Equals(dbid, JET_DBID.Nil) == false)
                Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);

            if (transaction != null)
                transaction.Dispose();

            if (session != null)
                session.Dispose();
        }

        public void Commit()
        {
            if (innerTxCount != 0)
                return;

            CommitCalled = true;
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

        public bool DoesTasksExistsForIndex(string name)
        {
            Api.JetSetCurrentIndex(session, tasks, "by_index");
            Api.MakeKey(session, tasks, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, tasks, SeekGrbit.SeekEQ) == false)
            {
                Api.MakeKey(session, tasks, "*", Encoding.Unicode, MakeKeyGrbit.NewKey);
                return Api.TrySeek(session, tasks, SeekGrbit.SeekEQ);
            }
            return true;
        }

        public IEnumerable<DocumentAndId> DocumentsById(int startId, int endId, int limit)
        {
            Api.JetSetCurrentIndex(session, documents, "by_id");
            Api.MakeKey(session, documents, startId, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekGE) == false)
            {
                logger.DebugFormat("Document with id {0} or higher was not found", startId);
                yield break;
            }
            Api.MakeKey(session, documents, endId, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(session, documents,
                                 SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

            int count = 0;
            while (count < limit)
            {
                count++;
                var id = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"],
                                                   RetrieveColumnGrbit.RetrieveFromIndex).Value;
                if (id > endId)
                    break;

                var data = Api.RetrieveColumnAsString(session, documents, documentsColumns["data"], Encoding.Unicode);
                logger.DebugFormat("Document with id '{0}' was found, doc length: {1}", id, data.Length);
                yield return new DocumentAndId { Document = data, Id = id };
            }
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

        public void DeleteDocument(string key)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
                return;
            }

            Api.JetDelete(session, documents);
            logger.DebugFormat("Document with key '{0}' was deleted", key);
        }

        public void AddTask(Task task)
        {
            using (var update = new Update(session, tasks, JET_prep.Insert))
            {
                Api.SetColumn(session, tasks, tasksColumns["task"], task.AsString(), Encoding.Unicode);
                Api.SetColumn(session, tasks, tasksColumns["for_index"], task.View, Encoding.Unicode);

                update.Save();
            }
            logger.DebugFormat("New task '{0}'", task);

        }

        public Task GetTask()
        {
            Api.MoveBeforeFirst(session, tasks);
            while (Api.TryMoveNext(session, tasks))
            {
                try
                {
                    Api.JetGetLock(session, tasks, GetLockGrbit.Write);
                }
                catch (EsentErrorException e)
                {
                    if (e.Error != JET_err.WriteConflict)
                        throw;
                }
                var task = Api.RetrieveColumnAsString(session, tasks, tasksColumns["task"], Encoding.Unicode);
                return Task.ToTask(task);
            }
            return null;
        }

        public void CompleteCurrentTask()
        {
            Api.JetDelete(session, tasks);
        }

        public void PushTx()
        {
            innerTxCount++;
        }

        public void PopTx()
        {
            innerTxCount--;
        }
    }
}