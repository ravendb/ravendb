using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Tasks;

namespace Raven.Database.Storage
{
    [CLSCompliant(false)]
    public class DocumentStorageActions : IDisposable
    {
        private readonly IDictionary<string, JET_COLUMNID> mappedResultsColumns;
        protected readonly JET_DBID dbid;
        protected readonly Table documents;
        protected readonly IDictionary<string, JET_COLUMNID> documentsColumns;
        protected readonly Table files;
        protected readonly IDictionary<string, JET_COLUMNID> filesColumns;
        private readonly Table indexesStats;
        private readonly IDictionary<string, JET_COLUMNID> indexesStatsColumns;
        protected readonly ILog logger = LogManager.GetLogger(typeof (DocumentStorageActions));
        protected readonly Session session;
        protected readonly Table tasks;
        protected readonly IDictionary<string, JET_COLUMNID> tasksColumns;
        private readonly Transaction transaction;
        private int innerTxCount;
        private Table mappedResults;

        [CLSCompliant(false)]
        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        public DocumentStorageActions(JET_INSTANCE instance,
                                      string database,
                                      IDictionary<string, JET_COLUMNID> documentsColumns,
                                      IDictionary<string, JET_COLUMNID> tasksColumns,
                                      IDictionary<string, JET_COLUMNID> filesColumns,
                                      IDictionary<string, JET_COLUMNID> indexesStatsColumns,
                                      IDictionary<string, JET_COLUMNID> mappedResultsColumns
            )
        {
            try
            {
                session = new Session(instance);
                transaction = new Transaction(session);
                Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

                documents = new Table(session, dbid, "documents", OpenTableGrbit.None);
                tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None);
                files = new Table(session, dbid, "files", OpenTableGrbit.None);
                indexesStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None);
                mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None);

                this.documentsColumns = documentsColumns;
                this.tasksColumns = tasksColumns;
                this.filesColumns = filesColumns;
                this.indexesStatsColumns = indexesStatsColumns;
                this.mappedResultsColumns = mappedResultsColumns;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public bool CommitCalled { get; set; }

        public IEnumerable<string> DocumentKeys
        {
            get
            {
                Api.MoveBeforeFirst(session, documents);
                while (Api.TryMoveNext(session, documents))
                {
                    yield return
                        Api.RetrieveColumnAsString(session, documents, documentsColumns["key"], Encoding.Unicode);
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            if(mappedResults != null)
                mappedResults.Dispose();

            if (indexesStats != null)
                indexesStats.Dispose();

            if (files != null)
                files.Dispose();

            if (documents != null)
                documents.Dispose();

            if (tasks != null)
                tasks.Dispose();

            if (Equals(dbid, JET_DBID.Nil) == false && session != null)
                Api.JetCloseDatabase(session.JetSesid, dbid, CloseDatabaseGrbit.None);

            if (transaction != null)
                transaction.Dispose();

            if (session != null)
                session.Dispose();
        }

        #endregion

        public JsonDocument DocumentByKey(string key)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found", key);
                return null;
            }
            var data = Api.RetrieveColumn(session, documents, documentsColumns["data"]);
            logger.DebugFormat("Document with key '{0}' was found", key);
            return new JsonDocument
            {
                Data = data,
                Etag = new Guid(Api.RetrieveColumn(session, documents, documentsColumns["etag"])),
                Key = Api.RetrieveColumnAsString(session, documents, documentsColumns["key"], Encoding.Unicode),
                Metadata = JObject.Parse(Api.RetrieveColumnAsString(session, documents, documentsColumns["metadata"]))
            };
        }

        public void Commit()
        {
            if (innerTxCount != 0)
                return;

            CommitCalled = true;
            transaction.Commit(CommitTransactionGrbit.None);
        }


        public Tuple<int, int> FirstAndLastDocumentKeys()
        {
            var item1 = 0;
            var item2 = 0;
            Api.MoveBeforeFirst(session, documents);
            if (Api.TryMoveNext(session, documents))
                item1 = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"]).Value;
            Api.MoveAfterLast(session, documents);
            if (Api.TryMovePrevious(session, documents))
                item2 = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"]).Value;
            return new Tuple<int, int>(item1, item2);
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

        public IEnumerable<Tuple<JsonDocument, int>> DocumentsById(Reference<bool> hasMoreWork, int startId, int endId,
                                                                   int limit)
        {
            Api.JetSetCurrentIndex(session, documents, "by_id");
            Api.MakeKey(session, documents, startId, MakeKeyGrbit.NewKey);
            // this sholdn't really happen, it means that the doc is missing
            // probably deleted before we can get it?
            if (Api.TrySeek(session, documents, SeekGrbit.SeekGE) == false)
            {
                logger.DebugFormat("Document with id {0} or higher was not found", startId);
                yield break;
            }
            var count = 0;
            do
            {
                if ((++count) > limit)
                {
                    hasMoreWork.Value = true;
                    yield break;
                }
                var id = Api.RetrieveColumnAsInt32(session, documents, documentsColumns["id"],
                                                   RetrieveColumnGrbit.RetrieveFromIndex).Value;
                if (id > endId)
                    break;

                var data = Api.RetrieveColumn(session, documents, documentsColumns["data"]);
                logger.DebugFormat("Document with id '{0}' was found, doc length: {1}", id, data.Length);
                var json = Api.RetrieveColumnAsString(session, documents, documentsColumns["metadata"],
                                                      Encoding.Unicode);
                var doc = new JsonDocument
                {
                    Key = Api.RetrieveColumnAsString(session, documents, documentsColumns["key"], Encoding.Unicode),
                    Data = data,
                    Etag = new Guid(Api.RetrieveColumn(session, documents, documentsColumns["etag"])),
                    Metadata = JObject.Parse(json)
                };
                yield return new Tuple<JsonDocument, int>(doc, id);
            } while (Api.TryMoveNext(session, documents));
            hasMoreWork.Value = false;
        }

        public void AddDocument(string key, string data, Guid? etag, string metadata)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            var isUpdate = Api.TrySeek(session, documents, SeekGrbit.SeekEQ);
            if (isUpdate)
            {
                var existingEtag = new Guid(Api.RetrieveColumn(session, documents, documentsColumns["etag"]));
                if (existingEtag != etag && etag != null)
                {
                    throw new ConcurrencyException("PUT attempted on document '" + key +
                                                   "' using a non current etag")
                    {
                        ActualETag = etag.Value,
                        ExpectedETag = existingEtag
                    };
                }
            }
            Guid newEtag;
            DocumentDatabase.UuidCreateSequential(out newEtag);

            using (var update = new Update(session, documents, isUpdate ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, documents, documentsColumns["key"], key, Encoding.Unicode);
                Api.SetColumn(session, documents, documentsColumns["data"], Encoding.UTF8.GetBytes(data));
                Api.SetColumn(session, documents, documentsColumns["etag"], newEtag.ToByteArray());
                Api.SetColumn(session, documents, documentsColumns["metadata"], metadata, Encoding.Unicode);

                update.Save();
            }
            logger.DebugFormat("Inserted a new document with key '{0}', doc length: {1}, update: {2}, ",
                               key, data.Length, isUpdate);
        }

        public void DeleteDocument(string key, Guid? etag)
        {
            Api.JetSetCurrentIndex(session, documents, "by_key");
            Api.MakeKey(session, documents, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, documents, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Document with key '{0}' was not found, and considered deleted", key);
                return;
            }

            var rowEtag = new Guid(Api.RetrieveColumn(session, documents, documentsColumns["etag"]));
            if (rowEtag != etag && etag != null)
            {
                throw new ConcurrencyException("DELETE attempted on document '" + key +
                                               "' using a non current etag")
                {
                    ActualETag = etag.Value,
                    ExpectedETag = rowEtag
                };
            }

            Api.JetDelete(session, documents);
            logger.DebugFormat("Document with key '{0}' was deleted", key);
        }

        public void AddTask(Task task)
        {
            using (var update = new Update(session, tasks, JET_prep.Insert))
            {
                Api.SetColumn(session, tasks, tasksColumns["task"], task.AsString(), Encoding.Unicode);
                Api.SetColumn(session, tasks, tasksColumns["for_index"], task.Index, Encoding.Unicode);

                update.Save();
            }
            if (logger.IsDebugEnabled)
                logger.DebugFormat("New task '{0}'", task.AsString());
        }

        public void AddAttachment(string key, Guid? etag, byte[] data, string headers)
        {
            Api.JetSetCurrentIndex(session, files, "by_name");
            Api.MakeKey(session, files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            var isUpdate = Api.TrySeek(session, files, SeekGrbit.SeekEQ);
            if (isUpdate)
            {
                var existingEtag = new Guid(Api.RetrieveColumn(session, files, filesColumns["etag"]));
                if (existingEtag != etag && etag != null)
                {
                    throw new ConcurrencyException("PUT attempted on attachment '" + key +
                                                   "' using a non current etag")
                    {
                        ActualETag = etag.Value,
                        ExpectedETag = existingEtag
                    };
                }
            }

            Guid newETag;
            DocumentDatabase.UuidCreateSequential(out newETag);
            using (var update = new Update(session, files, isUpdate ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, files, filesColumns["name"], key, Encoding.Unicode);
                Api.SetColumn(session, files, filesColumns["data"], data);
                Api.SetColumn(session, files, filesColumns["etag"], newETag.ToByteArray());
                Api.SetColumn(session, files, filesColumns["metadata"], headers, Encoding.Unicode);

                update.Save();
            }
            logger.DebugFormat("Adding attachment {0}", key);
        }

        public void DeleteAttachment(string key, Guid? etag)
        {
            Api.JetSetCurrentIndex(session, files, "by_name");
            Api.MakeKey(session, files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, files, SeekGrbit.SeekEQ) == false)
            {
                logger.DebugFormat("Attachment with key '{0}' was not found, and considered deleted", key);
                return;
            }
            var fileEtag = new Guid(Api.RetrieveColumn(session, files, filesColumns["etag"]));
            if (fileEtag != etag && etag != null)
            {
                throw new ConcurrencyException("DELETE attempted on attachment '" + key +
                                               "' using a non current etag")
                {
                    ActualETag = etag.Value,
                    ExpectedETag = fileEtag
                };
            }

            Api.JetDelete(session, files);
            logger.DebugFormat("Attachment with key '{0}' was deleted", key);
        }

        public Attachment GetAttachment(string key)
        {
            Api.JetSetCurrentIndex(session, files, "by_name");
            Api.MakeKey(session, files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, files, SeekGrbit.SeekEQ) == false)
            {
                return null;
            }

            var metadata = Api.RetrieveColumnAsString(session, files, filesColumns["metadata"], Encoding.Unicode);
            return new Attachment
            {
                Data = Api.RetrieveColumn(session, files, filesColumns["data"]),
                Etag = new Guid(Api.RetrieveColumn(session, files, filesColumns["etag"])),
                Metadata = JObject.Parse(metadata)
            };
        }

        public string GetFirstTask()
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
                return Api.RetrieveColumnAsString(session, tasks, tasksColumns["task"], Encoding.Unicode);
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

        public int GetDocumentsCount()
        {
            int val;
            Api.JetIndexRecordCount(session, documents, out val, 0);
            return val;
        }

        public void SetCurrentIndexStatsTo(string index)
        {
            Api.JetSetCurrentIndex(session, indexesStats, "by_key");
            Api.MakeKey(session, indexesStats, index, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, indexesStats, SeekGrbit.SeekEQ) == false)
                throw new InvalidOperationException("There is no index named: " + index);
        }

        public void IncrementIndexingAttempt()
        {
            Api.EscrowUpdate(session, indexesStats, indexesStatsColumns["attempts"], 1);
        }

        public void IncrementSuccessIndexing()
        {
            Api.EscrowUpdate(session, indexesStats, indexesStatsColumns["successes"], 1);
        }

        public void IncrementIndexingFailure()
        {
            Api.EscrowUpdate(session, indexesStats, indexesStatsColumns["errors"], 1);
        }

        public IEnumerable<IndexStats> GetIndexesStats()
        {
            Api.MoveBeforeFirst(session, indexesStats);
            while (Api.TryMoveNext(session, indexesStats))
            {
                yield return new IndexStats
                {
                    Name = Api.RetrieveColumnAsString(session, indexesStats, indexesStatsColumns["key"]),
                    IndexingAttempts =
                        Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["attempts"]).Value,
                    IndexingSuccesses =
                        Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["successes"]).Value,
                    IndexingErrors =
                        Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["errors"]).Value,
                };
            }
        }

        public void AddIndex(string name)
        {
            using (var update = new Update(session, indexesStats, JET_prep.Insert))
            {
                Api.SetColumn(session, indexesStats, indexesStatsColumns["key"], name, Encoding.Unicode);

                update.Save();
            }
        }

        public void DeleteIndex(string name)
        {
            Api.JetSetCurrentIndex(session, indexesStats, "by_key");
            Api.MakeKey(session, indexesStats, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, indexesStats, SeekGrbit.SeekEQ) == false)
                return;
            Api.JetDelete(session, indexesStats);
        }

        public void DecrementIndexingAttempt()
        {
            Api.EscrowUpdate(session, indexesStats, indexesStatsColumns["attempts"], -1);
        }

        public IndexFailureInformation GetFailureRate(string index)
        {
            SetCurrentIndexStatsTo(index);
            return new IndexFailureInformation
            {
                Name = index,
                Attempts = Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["attempts"]).Value,
                Errors = Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["errors"]).Value,
                Successes = Api.RetrieveColumnAsInt32(session, indexesStats, indexesStatsColumns["successes"]).Value
            };
        }

        public void PutMappedResult(string view, string docId, string reduceKey, string data)
        {
            Api.JetSetCurrentIndex(session, mappedResults, "by_pk");
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, mappedResults, docId, Encoding.Unicode, MakeKeyGrbit.None);
            Api.MakeKey(session, mappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
            bool isUpdate = Api.TrySeek(session, mappedResults, SeekGrbit.SeekEQ);

            using (var update = new Update(session, mappedResults, isUpdate ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, mappedResults, mappedResultsColumns["view"], view, Encoding.Unicode);
                Api.SetColumn(session, mappedResults, mappedResultsColumns["document_key"], docId, Encoding.Unicode);
                Api.SetColumn(session, mappedResults, mappedResultsColumns["reduce_key"], reduceKey, Encoding.Unicode);
                Api.SetColumn(session, mappedResults, mappedResultsColumns["data"], Encoding.UTF8.GetBytes(data));

                update.Save();
            }
        }

        public IEnumerable<string> GetMappedResults(string view, string reduceKey)
        {
            Api.JetSetCurrentIndex(session, mappedResults, "by_view_and_reduce_key");
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, mappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
            if (Api.TrySeek(session, mappedResults, SeekGrbit.SeekEQ) == false)
                yield break;

            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, mappedResults, reduceKey, Encoding.Unicode, MakeKeyGrbit.None);
            Api.JetSetIndexRange(session, mappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

            do
            {
                byte[] bytes = Api.RetrieveColumn(session, mappedResults, mappedResultsColumns["data"]);
                yield return Encoding.UTF8.GetString(bytes);
            } while (Api.TryMoveNext(session, mappedResults));
        }

        public IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view)
        {
            Api.JetSetCurrentIndex(session, mappedResults, "by_view_and_doc_key");
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, mappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
            if (Api.TrySeek(session, mappedResults, SeekGrbit.SeekEQ) == false)
                return new string[0];

            var reduceKeys = new HashSet<string>();
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, mappedResults, documentId, Encoding.Unicode, MakeKeyGrbit.None);
            Api.JetSetIndexRange(session, mappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
            do
            {
                var reduceKey = Api.RetrieveColumnAsString(session, mappedResults, mappedResultsColumns["reduce_key"],
                                                           Encoding.Unicode);
                reduceKeys.Add(reduceKey);
                Api.JetDelete(session, mappedResults);
            } while (Api.TryMoveNext(session, mappedResults));
            return reduceKeys;
        }

        public void DeleteMappedResultsForView(string view)
        {
            Api.JetSetCurrentIndex(session, mappedResults, "by_view");
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, mappedResults, SeekGrbit.SeekEQ) == false)
                return;
            Api.MakeKey(session, mappedResults, view, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(session, mappedResults, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
            
            do
            {
                Api.JetDelete(session, mappedResults);
            } while (Api.TryMoveNext(session, mappedResults));
        }
    }
}