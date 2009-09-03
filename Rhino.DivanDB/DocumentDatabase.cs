using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Storage;

namespace Rhino.DivanDB
{
    public class DocumentDatabase
    {
        private readonly DocumentStorage storage;

        public DocumentDatabase(string path)
        {
            storage = new DocumentStorage(path);
            storage.Initialize();
        }

        public string AddDocument(JObject document)
        {
            string key = GetKeyFromDocumentOrGenerateNewOne(document);
            storage.Batch(actions =>
            {
                actions.AddDocument(key, document.ToString());
                actions.Commit();
            });
            return key;
        }

        private static string GetKeyFromDocumentOrGenerateNewOne(IDictionary<string, JToken> document)
        {
            string id = GetKeyFromDocumentOrNull(document);
            if (id != null)
                return id;
            Guid value;
            UuidCreateSequential(out value);
            return  value.ToString();
        }

        private static string GetKeyFromDocumentOrNull(IDictionary<string, JToken> document)
        {
            JToken idToken;
            if (document.TryGetValue("_id", out idToken))
            {
                return (string)((JValue) idToken).Value;
            }
            return null;
        }

        [SuppressUnmanagedCodeSecurity]
        [DllImport("rpcrt4.dll", SetLastError = true)]
        static extern int UuidCreateSequential(out Guid value);

        public void Dispose()
        {
            storage.Dispose();
        }

        public JObject DocumentByKey(string key)
        {
            string document = null;
            storage.Batch(actions =>
            {
                document = actions.DocumentByKey(key);
                actions.Commit();
            });

            if(document == null)
                return null;

            return JObject.Parse(document);
        }

        public void EditDocument(JObject document)
        {
            string key = GetKeyFromDocument(document);

            storage.Batch(actions =>
            {
                actions.DeleteDocument(key);
                actions.AddDocument(key, document.ToString());
                actions.Commit();
            });
        }

        private string GetKeyFromDocument(JObject document)
        {
            var key = GetKeyFromDocumentOrNull(document);
            if(key == null)
                throw new InvalidOperationException("'_id' is a mandatory property for editing documents");
            return key;
        }

        public void DeleteDocument(JObject document)
        {
            string key = GetKeyFromDocument(document);
            storage.Batch(actions =>
            {
                actions.DeleteDocument(key);
                actions.Commit();
            });
        }
    }
}