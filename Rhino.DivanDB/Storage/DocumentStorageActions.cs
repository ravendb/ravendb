using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
        private readonly JET_DBID dbid;
        private readonly Transaction transaction;
        private readonly Table documents;
        private readonly IDictionary<string, JET_COLUMNID> documentsColumns;
        private readonly Table views;
        private readonly IDictionary<string, JET_COLUMNID> viewsColumns;
        private readonly Table viewsTransformationQueues;
        private readonly IDictionary<string, JET_COLUMNID> viewsTransformationQueuesColumns;


        public DocumentStorageActions(JET_INSTANCE instance,
                                          string database,
                                          Guid instanceId)
        {
            session = new Session(instance);
            transaction = new Transaction(session);
            Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);

            documents = new Table(session, dbid, "documents", OpenTableGrbit.None);
            documentsColumns = Api.GetColumnDictionary(session, documents);

            views = new Table(session, dbid, "viewDefinitions", OpenTableGrbit.None);
            viewsColumns = Api.GetColumnDictionary(session, views);

            viewsTransformationQueues = new Table(session, dbid, "viewTransformationQueues", OpenTableGrbit.None);
            viewsTransformationQueuesColumns = Api.GetColumnDictionary(session, viewsTransformationQueues);
        }


        public void AddView(string name, string definition, byte[] compiled)
        {
            using (var update = new Update(session, views, JET_prep.Insert))
            {
                Api.SetColumn(session, views, viewsColumns["name"], name, Encoding.Unicode);
                Api.SetColumn(session, views, viewsColumns["definition"], definition, Encoding.Unicode);
                Api.SetColumn(session, views, viewsColumns["hash"], ComputeSha256Hash(definition), Encoding.Unicode);
                Api.SetColumn(session, views, viewsColumns["complied_assembly"], compiled);

                update.Save();
            }
            logger.DebugFormat("Inserted a new view '{0}'", name);
        }

        private string ComputeSha256Hash(string definition)
        {
            byte[] stream = Encoding.Unicode.GetBytes(definition);
            byte[] hashed = SHA256.Create().ComputeHash(stream);
            return Convert.ToBase64String(hashed);
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
            if (viewsTransformationQueues != null)
                viewsTransformationQueues.Dispose();

            if (views != null)
                views.Dispose();

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

        public string[] ListViews()
        {
            var viewNames = new List<string>();
            Api.MoveBeforeFirst(session, views);
            while (Api.TryMoveNext(session, views))
                viewNames.Add(Api.RetrieveColumnAsString(session, views, viewsColumns["name"], Encoding.Unicode));
            return viewNames.ToArray();
        }

        public string ViewDefinitionByName(string name)
        {
            Api.JetSetCurrentIndex(session, views, "by_name");
            Api.MakeKey(session, views, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, views, SeekGrbit.SeekEQ) == false)
                return null;
            return Api.RetrieveColumnAsString(session, views, viewsColumns["definition"], Encoding.Unicode);
        }

        public string ViewHashByName(string name)
        {
            Api.JetSetCurrentIndex(session, views, "by_name");
            Api.MakeKey(session, views, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, views, SeekGrbit.SeekEQ) == false)
                return null;
            return Api.RetrieveColumnAsString(session, views, viewsColumns["hash"], Encoding.Unicode);
        }

        public ViewDefinition ViewCompiledAssemblyByName(string name)
        {
            Api.JetSetCurrentIndex(session, views, "by_name");
            Api.MakeKey(session, views, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, views, SeekGrbit.SeekEQ) == false)
                return null;
            return new ViewDefinition
            {
                CompiledAssembly = Api.RetrieveColumn(session, views, viewsColumns["complied_assembly"]),
                Name = Api.RetrieveColumnAsString(session, views, viewsColumns["name"], Encoding.Unicode)
            };
        }

        public void CreateViewTable(string name, Type generatedType)
        {
            if (generatedType.GetProperty("Key") == null)
                throw new InvalidOperationException("Generated type must have a property called 'Key'");

            JET_TABLEID newViewTable;
            Api.JetCreateTable(session, dbid, "views_" + name, 16, 100, out newViewTable);
            try
            {
                JET_COLUMNID columnid;
                Api.JetAddColumn(session, newViewTable, "key", new JET_COLUMNDEF
                {
                    cbMax = 255,
                    coltyp = JET_coltyp.Text,
                    cp = JET_CP.Unicode,
                    grbit = ColumndefGrbit.ColumnTagged
                }, null, 0, out columnid);
                Api.JetAddColumn(session, newViewTable, "original_doc_key", new JET_COLUMNDEF
                {
                    cbMax = 255,
                    coltyp = JET_coltyp.Text,
                    cp = JET_CP.Unicode,
                    grbit = ColumndefGrbit.ColumnTagged
                }, null, 0, out columnid);
                Api.JetAddColumn(session, newViewTable, "data", new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                    grbit = ColumndefGrbit.ColumnTagged
                }, null, 0, out columnid);
                var indexDef = "+key\0\0";
                Api.JetCreateIndex(session, newViewTable, "pk", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length, 100);
                indexDef = "+original_doc_key\0\0";
                Api.JetCreateIndex(session, newViewTable, "by_original_doc_key", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length, 100);
            }
            finally
            {
                Api.JetCloseTable(session, newViewTable);
            }
        }

        public string DeleteView(string name)
        {
            Api.JetSetCurrentIndex(session, views, "by_name");
            Api.MakeKey(session, views, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, views, SeekGrbit.SeekEQ) == false)
                return null;
            string hash = Api.RetrieveColumnAsString(session, views, viewsColumns["hash"], Encoding.Unicode);
            Api.JetDelete(session, views);
            return hash;

        }

        public void DeleteViewTable(string name)
        {
            string tableViewName = "views_" + name;
            if (Api.GetTableNames(session, dbid).Contains(tableViewName) == false)
                return;
            Api.JetDeleteTable(session, dbid, tableViewName);
        }

        public IEnumerable<string> ViewRecordsByNameAndKey(string name, string key)
        {
            using (var viewTable = new Table(session, dbid, "views_" + name, OpenTableGrbit.ReadOnly))
            {
                var viewTableColumns = Api.GetColumnDictionary(session, viewTable);
                Api.JetSetCurrentIndex(session, viewTable, "pk");
                Api.MakeKey(session, viewTable, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, viewTable, SeekGrbit.SeekEQ) == false)
                    yield break;
                Api.MakeKey(session, viewTable, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                Api.JetSetIndexRange(session, viewTable,
                                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

                do
                {
                    yield return Api.RetrieveColumnAsString(session, viewTable, viewTableColumns["data"], Encoding.Unicode);
                } while (Api.TryMoveNext(session, viewTable));
            }
        }

        public IEnumerable<string> QueuedDocumentsFor(string name)
        {
            Api.JetSetCurrentIndex(session, viewsTransformationQueues, "by_view");
            Api.MakeKey(session, viewsTransformationQueues, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, viewsTransformationQueues, SeekGrbit.SeekEQ) == false)
                yield break;

            do
            {
                Api.MakeKey(session, viewsTransformationQueues, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
                if (Api.TrySeek(session, viewsTransformationQueues, SeekGrbit.SeekEQ) == false)
                    yield break;
             
                var doc = Api.RetrieveColumnAsString(session, viewsTransformationQueues,
                                                      viewsTransformationQueuesColumns["documentKey"], Encoding.Unicode);
                yield return doc;
                Api.JetDelete(session, viewsTransformationQueues);
            } while (Api.TryMoveNext(session, viewsTransformationQueues));

        }

        public void QueueDocumentForViewTransformation(string name, string key)
        {
            using (var update = new Update(session, viewsTransformationQueues, JET_prep.Insert))
            {
                Api.SetColumn(session, viewsTransformationQueues, viewsTransformationQueuesColumns["documentKey"], key, Encoding.Unicode);
                Api.SetColumn(session, viewsTransformationQueues, viewsTransformationQueuesColumns["viewName"], name, Encoding.Unicode);

                update.Save();
            }
        }

        public IEnumerable<string> DocumentKeys()
        {
            Api.MoveBeforeFirst(session, documents);
            while (Api.TryMoveNext(session, documents))
            {
                yield return Api.RetrieveColumnAsString(session, documents, documentsColumns["key"], Encoding.Unicode);
            }
        }

        public void AddViewRecord(string view, string key, string documentKey, string data)
        {
            using (var viewTable = new Table(session, dbid, "views_" + view, OpenTableGrbit.None))
            using (var update = new Update(session, viewTable, JET_prep.Insert))
            {
                var columns = Api.GetColumnDictionary(session, viewTable);
                Api.SetColumn(session, viewTable, columns["key"], key, Encoding.Unicode);
                Api.SetColumn(session, viewTable, columns["original_doc_key"], documentKey, Encoding.Unicode);
                Api.SetColumn(session, viewTable, columns["data"], data, Encoding.Unicode);

                update.Save();
            }
        }
    }
}