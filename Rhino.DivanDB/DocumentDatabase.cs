using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
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
        }

        public TransactionalStorage TransactionalStorage { get; private set; }
        public ViewStorage ViewStorage { get; private set; }
        public IndexStorage IndexStorage { get; private set; }

        #region IDisposable Members

        public void Dispose()
        {
            TransactionalStorage.Dispose();
            IndexStorage.Dispose();
        }

        #endregion

        private static string GetKeyFromDocumentOrGenerateNewOne(IDictionary<string, JToken> document)
        {
            string id = null;
            JToken idToken;
            if (document.TryGetValue("_id", out idToken))
            {
                id = (string) ((JValue) idToken).Value;
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
                                           actions.AddTask(new IndexDocumentRangeTask
                                                           {View = "*", FromKey = key, ToKey = key});
                                           actions.Commit();
                                       });
            return key;
        }

        public void Delete(string key)
        {
            TransactionalStorage.Write(actions =>
                                       {
                                           actions.DeleteDocument(key);
                                           actions.AddTask(new RemoveFromIndexTask {View = "*", Keys = new[] {key}});
                                           actions.Commit();
                                       });
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
        }

        public JObject[] Query(string index, string query)
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
            return list.ToArray();
        }

        public void DeleteView(string name)
        {
            ViewStorage.RemoveView(name);
            IndexStorage.DeleteIndex(name);
        }
    }
}