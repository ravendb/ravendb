using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Newtonsoft.Json;
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
        private readonly static string[] ReservedFields = new[] { "_docNum" };

        public DatabaseStatistics Statistics
        {
            get
            {
                var result = new DatabaseStatistics
                {
                    CountOfIndexes = IndexStorage.Indexes.Length
                };
                TransactionalStorage.Batch(actions =>
                {
                    result.CountOfDocuments = actions.GetDocumentsCount();
                    result.StaleIndexes = IndexStorage.Indexes
                        .Where(actions.DoesTasksExistsForIndex)
                        .ToArray();
                    actions.Commit();
                });
                return result;
            }
        }
        public TransactionalStorage TransactionalStorage { get; private set; }
        public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }
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

            foreach (var reservedField in ReservedFields)
            {
                document.Remove(reservedField);
            }

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

        public string PutIndex(string name, string indexDef)
        {
            LinqTransformer transformer;
            switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(name, indexDef, out transformer))
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
                                                               ToKey = firstAndLast.Second
                                                           });
                                           actions.Commit();
                                       });
            workContext.NotifyAboutWork();
            return transformer.Name;
        }

        public QueryResult Query(string index, string query, int start, int pageSize)
        {
            var list = new List<JObject>();
            var stale = false;
            var totalSize = new Reference<int>();
            TransactionalStorage.Batch(
                actions =>
                {
                    stale = actions.DoesTasksExistsForIndex(index);
                    list.AddRange(from key in IndexStorage.Query(index, query, start, pageSize, totalSize)
                                  select actions.DocumentByKey(key)
                                      into doc
                                      where doc != null
                                      select doc.ToJson());
                    actions.Commit();
                });
            return new QueryResult
                   {
                       Results = list.ToArray(),
                       IsStale = stale,
                       TotalResults = totalSize.Value
                   };
        }

        public void DeleteIndex(string name)
        {
            IndexDefinitionStorage.RemoveIndex(name);
            IndexStorage.DeleteIndex(name);
            workContext.NotifyAboutWork();
        }

        public Tuple<byte[], NameValueCollection> GetStatic(string name)
        {
            Tuple<byte[], string> attachment = null;
            TransactionalStorage.Batch(actions =>
            {
                attachment = actions.GetAttachment(name);
                actions.Commit();
            });
            if(attachment==null)
                return null;
            return new Tuple<byte[], NameValueCollection>
            {
                First = attachment.First,
                Second = GetHeadersAsNameValueString(attachment.Second)
            };
        }

        public void PutStatic(string name, byte[] data, NameValueCollection headers)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.AddAttachment(name, data,GetHeadersAsString(headers));
                actions.Commit();
            });
        }

        private static string GetHeadersAsString(NameValueCollection headers)
        {
            var writer = new StringWriter();
            var headersAdJson = new JObject(headers.AllKeys.Select(key=> new JProperty(key,headers[key])));
            headersAdJson.WriteTo(new JsonTextWriter(writer));
            return writer.GetStringBuilder().ToString();
        }


        private static NameValueCollection GetHeadersAsNameValueString(string headers)
        {
            var nvc = new NameValueCollection();
            foreach (var property in JObject.Parse(headers).Properties())
            {
                var value = property.Value.Value<object>() ?? "null";
                nvc.Add(property.Name, value.ToString());
            }
            return nvc;
        }

        public void DeleteStatic(string name)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.DeleteAttachment(name);
                actions.Commit();
            });
        }

        public JArray GetDocuments(int start, int pageSize)
        {
            var list = new JArray();
            TransactionalStorage.Batch(actions =>
            {
                foreach (var documentAndId in actions.DocumentsById(new Reference<bool>(), start, int.MaxValue, pageSize))
                {
                    var doc = JObject.Parse(documentAndId.First);
                    doc.Add("_docNum", new JValue(documentAndId.Second));

                    list.Add(doc);
                }
                actions.Commit();
            });
            return list;
        }

        public JArray GetIndexNames(int start, int pageSize)
        {
            return new JArray(
                IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                    .Select(s => new JValue(s))
                );
        }

        public JArray GetIndexes(int start, int pageSize)
        {
            return new JArray(
                IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
                    .Select(
                        indexName => new JObject
                        {
                            {"name", new JValue(indexName)},
                            {"definition", new JValue(IndexDefinitionStorage.GetIndexDefinition(indexName))}
                        })
                );
        }
    }
}