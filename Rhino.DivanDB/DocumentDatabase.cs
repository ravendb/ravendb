using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Indexing;
using Rhino.DivanDB.Linq;
using Rhino.DivanDB.Storage;
using Rhino.DivanDB.Tasks;
using Rhino.DivanDB.Extensions;

namespace Rhino.DivanDB
{
    public class DocumentDatabase : IDisposable
    {
        public DocumentDatabase(string path)
        {
            TransactionalStorage = new TransactionalStorage(path);
            try
            {
                TransactionalStorage.Initialize();
            }
            catch (Exception)
            {
                TransactionalStorage.Dispose();
                throw;
            }

            IndexDefinitionStorage = new IndexDefinitionStorage(path);
            IndexStorage = new IndexStorage(path);
            workContext = new WorkContext
                          {
                              IndexStorage = IndexStorage,
                              TransactionaStorage = TransactionalStorage,
                              IndexDefinitionStorage = IndexDefinitionStorage
                          };
        }

        public void SpinBackgroundWorkers()
        {
            var threadCount = 1;// Environment.ProcessorCount;
            backgroundWorkers = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                backgroundWorkers[i] = new Thread(new TaskExecuter(TransactionalStorage, workContext).Execute)
                                       {
                                           IsBackground = true,
                                           Name = "RDB Background Worker #" + i,
                                       };
                backgroundWorkers[i].Start();
            }
        }

        private Thread[] backgroundWorkers = new Thread[0];
        private readonly WorkContext workContext;
        public TransactionalStorage TransactionalStorage { get; private set; }
        public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }
        public IndexStorage IndexStorage { get; private set; }

        public int CountOfDocuments
        {
            get
            {
                int value = 0;
                TransactionalStorage.Batch(actions =>
                {
                    value = actions.GetDocumentsCount();
                    actions.Commit();
                });
                return value;
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            workContext.StopWork();
            TransactionalStorage.Dispose();
            IndexStorage.Dispose();
            foreach (var backgroundWorker in backgroundWorkers)
            {
                backgroundWorker.Join();
            }
        }

        #endregion

        private static string GetKeyFromDocumentOrGenerateNewOne(IDictionary<string, JToken> document)
        {
            string id = null;
            JToken idToken;
            if (document.TryGetValue("_id", out idToken))
            {
                id = (string)((JValue)idToken).Value;
            }
            if (id != null)
                return id;
            Guid value;
            UuidCreateSequential(out value);
            document.Add("_id", new JValue(value.ToString()));
            return value.ToString();
        }

        [SuppressUnmanagedCodeSecurity]
        [DllImport("rpcrt4.dll", SetLastError = true)]
        private static extern int UuidCreateSequential(out Guid value);

        public byte[] Get(string key)
        {
            byte[] document = null;
            TransactionalStorage.Batch(actions =>
                                      {
                                          document = actions.DocumentByKey(key);
                                          actions.Commit();
                                      });
            return document;
        }

        public string Put(JObject document)
        {
            string key = GetKeyFromDocumentOrGenerateNewOne(document);

            TransactionalStorage.Batch(actions =>
                                       {
                                           actions.DeleteDocument(key);
                                           actions.AddDocument(key, document.ToString());
                                           actions.AddTask(new IndexDocumentTask { View = "*", Key = key });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
            return key;
        }

        public void Delete(string key)
        {
            TransactionalStorage.Batch(actions =>
                                       {
                                           actions.DeleteDocument(key);
                                           actions.AddTask(new RemoveFromIndexTask { View = "*", Keys = new[] { key } });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
        }

        public string PutIndex(string indexDef)
        {
            LinqTransformer transformer;
            switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(indexDef, out transformer))
            {
                case IndexCreationOptions.Noop:
                    return transformer.Name;
                case IndexCreationOptions.Update:
                    DeleteIndex(transformer.Name);
                    break;
            }
            IndexDefinitionStorage.AddIndex(transformer);
            IndexStorage.CreateIndex(transformer.Name);
            TransactionalStorage.Batch(actions =>
                                       {
                                           var firstAndLast = actions.FirstAndLastDocumentKeys();
                                           actions.AddTask(new IndexDocumentRangeTask
                                                           {
                                                               View = transformer.Name,
                                                               FromKey = firstAndLast.First,
                                                               ToKey = firstAndLast.Last
                                                           });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
            return transformer.Name;
        }

        public QueryResult Query(string index, string query)
        {
            var list = new List<JObject>();
            var stale = false;
            TransactionalStorage.Batch(
                actions =>
                {
                    stale = actions.DoesTasksExistsForIndex(index);
                    list.AddRange(from key in IndexStorage.Query(index, query)
                                  select actions.DocumentByKey(key)
                                      into doc
                                      where doc != null
                                      select doc.ToJson());
                    actions.Commit();
                });
            return new QueryResult
                   {
                       Results = list.ToArray(),
                       IsStale = stale
                   };
        }

        public void DeleteIndex(string name)
        {
            IndexDefinitionStorage.RemoveIndex(name);
            IndexStorage.DeleteIndex(name);
            workContext.NotifyAboutWork();
        }

        public byte[] GetStatic(string name)
        {
            byte[] attachment = null;
            TransactionalStorage.Batch(actions =>
            {
                attachment = actions.GetAttachment(name);
                actions.Commit();
            });
            return attachment;
        }

        public void PutStatic(string name, byte[] data)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.AddAttachment(name, data);
                actions.Commit();
            });
        }

        public void DeleteStatic(string name)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.DeleteAttachment(name);
                actions.Commit();
            });
        }
    }

    public class QueryResult
    {
        public JObject[] Results { get; set; }
        public bool IsStale { get; set; }
    }
}