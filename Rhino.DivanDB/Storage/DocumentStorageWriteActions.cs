using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Tasks;

namespace Rhino.DivanDB.Storage
{
    public class DocumentStorageWriteActions : DocumentStorageActions
    {
        protected readonly Table tasks;
        protected readonly IDictionary<string, JET_COLUMNID> tasksColumns;


        [CLSCompliant(false)]
        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        public DocumentStorageWriteActions(JET_INSTANCE instance, string database)
            : base(instance, database)
        {
            try
            {
                tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None);
                tasksColumns = Api.GetColumnDictionary(session, tasks);
            }
            catch (Exception)
            {
                Dispose();
                throw;
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

                update.Save();
            }
            logger.DebugFormat("New task '{0}'", task);
    
        }

        public override void Dispose()
        {
            if(tasks!=null)
                tasks.Dispose();

            base.Dispose();
        }

        public Task GetTask()
        {
            Api.MoveBeforeFirst(session, tasks);
            while(Api.TryMoveNext(session, tasks))
            {
                try
                {
                    Api.JetGetLock(session, tasks, GetLockGrbit.Write);
                }
                catch(EsentErrorException e)
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
    }
}