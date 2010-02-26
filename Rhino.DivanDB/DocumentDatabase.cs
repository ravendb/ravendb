using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Indexing;
using Rhino.DivanDB.Storage;
using Rhino.DivanDB.Tasks;

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
            }

            ViewStorage = new ViewStorage(path);
            IndexStorage = new IndexStorage(path);
            workContext = new WorkContext
                          {
                              IndexStorage = IndexStorage,
                              TransactionaStorage = TransactionalStorage,
                              ViewStorage = ViewStorage
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
        public ViewStorage ViewStorage { get; private set; }
        public IndexStorage IndexStorage { get; private set; }

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

        public JObject Get(string key)
        {
            string document = null;
            TransactionalStorage.Read(actions =>
                                      {
                                          document = actions.DocumentByKey(key);
                                          actions.Commit();
                                      });

            if (document == null)
                return null;

            return JObject.Parse(document);
        }

        public string Put(JObject document)
        {
            string key = GetKeyFromDocumentOrGenerateNewOne(document);

            TransactionalStorage.Write(actions =>
                                       {
                                           actions.DeleteDocument(key);
                                           actions.AddDocument(key, document.ToString());
                                           actions.AddTask(new IndexDocumentTask { Key = key });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
            return key;
        }

        public void Delete(string key)
        {
            TransactionalStorage.Write(actions =>
                                       {
                                           actions.DeleteDocument(key);
                                           actions.AddTask(new RemoveFromIndexTask { View = "*", Keys = new[] { key } });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
        }

        public void AddView(string viewDefinition)
        {
            string viewName = ViewStorage.AddView(viewDefinition);
            IndexStorage.CreateIndex(viewName);
            TransactionalStorage.Write(actions =>
                                       {
                                           var firstAndLast = actions.FirstAndLastDocumentKeys();
                                           actions.AddTask(new IndexDocumentRangeTask
                                                           {
                                                               View = viewName,
                                                               FromKey = firstAndLast.First,
                                                               ToKey = firstAndLast.Last
                                                           });
                                       });
            workContext.NotifyAboutWork();
        }

        public QueryResult Query(string index, string query)
        {
            var list = new List<JObject>();
            TransactionalStorage.Read(
                actions =>
                {
                    list.AddRange(from key in IndexStorage.Query(index, query)
                                  select actions.DocumentByKey(key)
                                      into doc
                                      where doc != null
                                      select JObject.Parse(doc));
                });
            return new QueryResult
                   {
                       Results = list.ToArray(),
                       IsStale = false
                   };
        }

        public void DeleteView(string name)
        {
            ViewStorage.RemoveView(name);
            IndexStorage.DeleteIndex(name);
        }
    }

    public class QueryResult
    {
        public JObject[] Results { get; set; }
        public bool IsStale { get; set; }
    }
}