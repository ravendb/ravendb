using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;

namespace Raven.Bundles.Replication
{
    public class ReplicationTask : IRequiresDocumentDatabaseInitialization
    {
        private DocumentDatabase docDb;
        private readonly ILog log = LogManager.GetLogger(typeof(ReplicationTask));
        private bool firstTimeFoundNoReplicationDocument = true;

        public void Initialize(DocumentDatabase database)
        {
            docDb = database;

            new Thread(Execute)
            {
                IsBackground = true,
                Name = "Replication Thread"
            }.Start();

        }

        private void Execute()
        {
            var context = docDb.WorkContext;
            while (context.DoWork)
            {
                try
                {
                    var destinations = GetReplicationDestinations();

                    if (destinations.Length == 0)
                    {
                        WarnIfNoReplicationTargetsWereFound();
                        continue;
                    }
                    
                    foreach (var destination in destinations)
                    {
                        ReplicateTo(destination);
                    }
                }
                catch (Exception e)
                {
                    log.Error("Failed to perform replication", e);
                }

                context.WaitForWork();
            }
        }

        private void WarnIfNoReplicationTargetsWereFound()
        {
            if (firstTimeFoundNoReplicationDocument)
            {
                firstTimeFoundNoReplicationDocument = false;
                log.Warn(
                    "Replication bundle is installed, but there is no destination in 'Raven/Replication/Destinations'.\r\nRepliaction results in NO-OP");
            }
        }

        private void ReplicateTo(string destination)
        {
            var etag = GetLastReplicatedEtagFrom(destination);
            if(etag == null)
                return;
            var jsonDocuments = GetJsonDocuments(etag.Value);
            if(jsonDocuments == null)
                return;
            TryReplicatingData(destination, jsonDocuments);
        }

        private void TryReplicatingData(string destination, JArray jsonDocuments)
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(destination + "/replication/replicate?from=" + docDb.Configuration.ServerUrl);
                request.UseDefaultCredentials = true;
                request.Timeout = 500;
                request.Credentials = CredentialCache.DefaultNetworkCredentials;
                request.Method = "POST";
                using(var stream = request.GetRequestStream())
                using (var streamWriter = new StreamWriter(stream))
                {
                    jsonDocuments.WriteTo(new JsonTextWriter(streamWriter));
                    streamWriter.Flush();
                    stream.Flush();
                }
                using (request.GetResponse())
                {
                    log.InfoFormat("Replicated {0} to {1}", jsonDocuments.Count, destination);
                }
            }
            catch (Exception e)
            {
                log.Warn("Replication to " + destination + " had failed", e);
            }
        }

        private JArray GetJsonDocuments(Guid etag)
        {
            JArray jsonDocuments = null;
            try
            {
                docDb.TransactionalStorage.Batch(actions =>
                {
                    jsonDocuments = new JArray(actions.GetDocumentsAfter(etag).Take(100).Select(x => x.ToJson()));
                });
            }
            catch (Exception e)
            {
                log.Warn("Could not get documents to replicate after: " + etag, e);
            }
            return jsonDocuments;
        }

        private Guid? GetLastReplicatedEtagFrom(string destination)
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(destination + "/replication/lastEtag?from=" + docDb.Configuration.ServerUrl);
                request.UseDefaultCredentials = true;
                request.Timeout = 500;
                request.Credentials = CredentialCache.DefaultNetworkCredentials;
                using (var response = request.GetResponse())
                using (var stream = response.GetResponseStream())
                {
                    var etagFromServer = (EtagFromServer)new JsonSerializer().Deserialize(new StreamReader(stream), typeof(EtagFromServer));
                    return etagFromServer.Etag;
                }
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if(response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode==HttpStatusCode.NotFound))
                    log.Warn("Replication is not enabled on: " + destination, e);
                else
                    log.Warn("Failed to contact replication destination: " + destination, e);
            }
            catch (Exception e)
            {
                log.Warn("Failed to contact replication destination: " + destination, e);
            }
            return null;
        }

        private class EtagFromServer
        {
            public Guid Etag { get; set; }
        }

        private string[] GetReplicationDestinations()
        {
            var document = docDb.Get(ReplicationConstants.RavenReplicationDestinations, null);
            if (document == null)
            {
                docDb.Put(ReplicationConstants.RavenReplicationDestinations, null, new JObject(), new JObject(), null);
                document = docDb.Get(ReplicationConstants.RavenReplicationDestinations, null);
            }
            return document.DataAsJson.Cast<JProperty>().Select(x => x.Value<string>()).ToArray();
        }
    }
}