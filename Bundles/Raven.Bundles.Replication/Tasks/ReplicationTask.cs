using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Json;

namespace Raven.Bundles.Replication.Tasks
{
    public class ReplicationTask : IStartupTask
    {
        private DocumentDatabase docDb;
        private readonly ILog log = LogManager.GetLogger(typeof(ReplicationTask));
        private bool firstTimeFoundNoReplicationDocument = true;

        private int replicationAttempts;

        public void Execute(DocumentDatabase database)
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
                    using (ReplicationContext.Enter())
                    {
                        var destinations = GetReplicationDestinations();

                        if (destinations.Length == 0)
                        {
                            WarnIfNoReplicationTargetsWereFound();
                        }
                        else
                        {
                            var currentReplicationAttempts = Interlocked.Increment(ref replicationAttempts);
                            Parallel.ForEach(destinations.Where(dest=>ShouldReplicateTo(dest, currentReplicationAttempts)), 
                                ReplicateTo);
                        }
                    }
                }
                catch (Exception e)
                {
                    log.Error("Failed to perform replication", e);
                }

                context.WaitForWork(TimeSpan.FromMinutes(1));
            }
        }

        private bool ShouldReplicateTo(string dest, int currentReplicationAttempts)
        {
            var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath + dest, null);
            if (jsonDocument == null)
                return true;
            var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
            if (failureInformation.FailureCount > 1000)
            {
                var shouldReplicateTo = currentReplicationAttempts%1000 == 0;
                log.DebugFormat("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 100)
            {
                var shouldReplicateTo = currentReplicationAttempts % 100 == 0;
                log.DebugFormat("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            if (failureInformation.FailureCount > 10)
            {
                var shouldReplicateTo = currentReplicationAttempts % 10 == 0;
                log.DebugFormat("Failure count for {0} is {1}, skipping replication: {2}",
                    dest, failureInformation.FailureCount, shouldReplicateTo == false);
                return shouldReplicateTo;
            }
            return true;
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
            using (ReplicationContext.Enter())
            {
                JArray jsonDocuments;
                try
                {
                    var etag = GetLastReplicatedEtagFrom(destination);
                    if (etag == null)
                        return;
                    jsonDocuments = GetJsonDocuments(etag.Value);
                    if (jsonDocuments == null || jsonDocuments.Count == 0)
                        return;
                }
                catch (Exception e)
                {
                    log.Warn("Failed to replicate to: " + destination, e);
                    return;
                }
                if (TryReplicatingData(destination, jsonDocuments) == false)// failed to replicate, start error handling strategy
                {
                    if (IsFirstFailue(destination))
                    {
                        log.InfoFormat(
                            "This is the first failure for {0}, assuming transinet failure and trying again",
                            destination);
                        if (TryReplicatingData(destination, jsonDocuments))// second failure!
                            return;
                    }
                    IncrementFailureCount(destination);
                }
            }
        }

        private void IncrementFailureCount(string destination)
        {
            var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath + destination, null);
            var failureInformation = new DestinationFailureInformation {Destination = destination};
            if (jsonDocument != null)
            {
                failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
            }
            failureInformation.FailureCount += 1;
            docDb.Put(ReplicationConstants.RavenReplicationDestinationsBasePath + destination, null,
                      JObject.FromObject(failureInformation), new JObject(), null);
        }

        private bool IsFirstFailue(string destination)
        {
            var jsonDocument = docDb.Get(ReplicationConstants.RavenReplicationDestinationsBasePath+destination, null);
            if (jsonDocument == null)
                return true;
            var failureInformation = jsonDocument.DataAsJson.JsonDeserialization<DestinationFailureInformation>();
            return failureInformation.FailureCount == 0;
        }

        private bool TryReplicatingData(string destination, JArray jsonDocuments)
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(destination + "/replication/replicate?from=" + docDb.Configuration.ServerUrl);
                request.UseDefaultCredentials = true;
                request.Credentials = CredentialCache.DefaultNetworkCredentials;
                request.Method = "POST";
                using (var stream = request.GetRequestStream())
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
                return true;
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response != null)
                {
                    using (var streamReader = new StreamReader(response.GetResponseStream()))
                    {
                        var error = streamReader.ReadToEnd();
                        log.Warn("Replication to " + destination + " had failed\r\n" + error, e);
                    }
                }
                else
                {
                    log.Warn("Replication to " + destination + " had failed", e);
                }
                return false;
            }
            catch (Exception e)
            {
                log.Warn("Replication to " + destination + " had failed", e);
                return false;
            }
        }

        private JArray GetJsonDocuments(Guid etag)
        {
            JArray jsonDocuments = null;
            try
            {
                var instanceId = docDb.TransactionalStorage.Id.ToString();
                docDb.TransactionalStorage.Batch(actions =>
                {
                    jsonDocuments = new JArray(actions.GetDocumentsAfter(etag)
                        .Where(x => x.Key.StartsWith("Raven/") == false)
                        .Where(x => x.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) == instanceId)
                        .Take(100)
                        .Select(x => x.ToJson()));
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
                if (response != null && (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound))
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
                docDb.Put(ReplicationConstants.RavenReplicationDestinations, null, JObject.FromObject(new ReplicationDocument()), new JObject(), null);
                document = docDb.Get(ReplicationConstants.RavenReplicationDestinations, null);
            }
            return document.DataAsJson.JsonDeserialization<ReplicationDocument>()
                .Destinations.Select(x => x.Url)
                .ToArray();
        }
    }
}