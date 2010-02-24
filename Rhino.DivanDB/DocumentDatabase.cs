using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Storage;
using System.Linq;

namespace Rhino.DivanDB
{
    public class DocumentDatabase : IDisposable
    {
        public DocumentStorage DocumentStorage { get; private set; }
        public ViewStorage ViewStorage { get; private set; }
        public IndexStorage IndexStorage { get; private set; }
        private object writeLock = new object();

        public DocumentDatabase(string path)
        {
            DocumentStorage = new DocumentStorage(path);
            try
            {
                DocumentStorage.Initialize();
            }
            catch (Exception)
            {
                DocumentStorage.Dispose();
            }

            ViewStorage = new ViewStorage(path);
            IndexStorage = new IndexStorage(path);
        }

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
            return  value.ToString();
        }

        [SuppressUnmanagedCodeSecurity]
        [DllImport("rpcrt4.dll", SetLastError = true)]
        static extern int UuidCreateSequential(out Guid value);

        public void Dispose()
        {
            DocumentStorage.Dispose();
            IndexStorage.Dispose();
        }

        public JObject Get(string key)
        {
            string document = null;
            DocumentStorage.Read(actions =>
            {
                document = actions.DocumentByKey(key);
                actions.Commit();
            });

            if(document == null)
                return null;

            return JObject.Parse(document);
        }

        public string Put(JObject document)
        {
           lock(writeLock)
           {
               string key = GetKeyFromDocumentOrGenerateNewOne(document);

               DocumentStorage.Write(actions =>
               {
                   actions.DeleteDocument(key);
                   actions.AddDocument(key, document.ToString());
                   actions.Commit();
               });
               return key;
           }
        }

        public void Delete(string key)
        {
           lock(writeLock)
           {
               DocumentStorage.Write(actions =>
               {
                   actions.DeleteDocument(key);
                   actions.Commit();
               });
               IndexStorage.Delete(key);
           }
        }

        public void AddView(string viewDefinition)
        {
            lock(writeLock)
            {
                var viewFunc = ViewStorage.AddView(viewDefinition);
                DocumentStorage.Write(actions =>
                {
                    var results = viewFunc(actions.DocumentKeys
                                                                  .Select(key => actions.DocumentByKey(key))
                                                                  .Select(s => new JsonDynamicObject(s)));
                    IndexStorage.Index(results);
                });
            }
        }
    }
}