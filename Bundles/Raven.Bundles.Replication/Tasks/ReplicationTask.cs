using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Replication.Data;
using Raven.Database;
using Raven.Database.Json;

namespace Raven.Bundles.Replication.Tasks
{
    public class ReplicationTask : IStartupTask
    {
        public class IntHolder
        {
            public int Value;
        }

        private DocumentDatabase docDb;
        private readonly ILog log = LogManager.GetLogger(typeof(ReplicationTask));
        private bool firstTimeFoundNoReplicationDocument = true;
        private readonly ConcurrentDictionary<string, IntHolder> activeReplicationTasks = new ConcurrentDictionary<string, IntHolder>();

        private int replicationAttempts;
        private int workCounter;

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

                            var destinationForReplication = destinations
                                .Where(dest => IsNotFailing(dest, currentReplicationAttempts));

                            foreach (var dest in destinationForReplication)
                            {
                                var destination = dest;
                                var holder = activeReplicationTasks.GetOrAdd(destination, new IntHolder());
                                if (Thread.VolatileRead(ref holder.Value) == 1)
                                    continue;
                                Thread.VolatileWrite(ref holder.Value, 1);
                                var task = new Task<bool>(() => ReplicateTo(destination), TaskCreationOptions.LongRunning);
                                task.Start();
                                task.ContinueWith(completedTask =>
                                {
                                    if (completedTask.Result) // force re-evaluation of replication again
                                        docDb.WorkContext.NotifyAboutWork();
                                });
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    log.Error("Failed to perform replication", e);
                }

                context.WaitForWork(TimeSpan.FromMinutes(1), ref workCounter);
            }
        }

        private bool IsNotFailing(string dest, int currentReplicationAttempts)
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

        private bool ReplicateTo(string destination)
        {
            try
            {
                using (ReplicationContext.Enter())
                {
                    SourceReplicationInformation sourceReplicationInformation;
                    try
                    {
                        sourceReplicationInformation = GetLastReplicatedEtagFrom(destination);
                        if (sourceReplicationInformation == null)
                            return false;
                    }
                    catch (Exception e)
                    {
                        log.Warn("Failed to replicate to: " + destination, e);
                        return false;
                    }

                    if (ReplicateDocuments(destination, sourceReplicationInformation) == false)
                        return false;

                    if (ReplicateAttachments(destination, sourceReplicationInformation) == false)
                        return false;

                    return true;
                }
            }
            finally 
            {
                var holder = activeReplicationTasks.GetOrAdd(destination, new IntHolder());
                Thread.VolatileWrite(ref holder.Value, 0);
            }
        }

        private bool? ReplicateAttachments(string destination, SourceReplicationInformation sourceReplicationInformation)
        {
            var attachments = GetAttachments(sourceReplicationInformation.LastAttachmentEtag);

            if (attachments == null || attachments.Count == 0)
                return null;

            if (TryReplicationAttachments(destination, attachments) == false)// failed to replicate, start error handling strategy
            {
                if (IsFirstFailue(destination))
                {
                    log.InfoFormat(
                        "This is the first failure for {0}, assuming transinet failure and trying again",
                        destination);
                    if (TryReplicationAttachments(destination, attachments))// success on second faile
                        return true;
                }
                IncrementFailureCount(destination);
                return false;
            }

            return true;
        }

        private bool? ReplicateDocuments(string destination, SourceReplicationInformation sourceReplicationInformation)
        {
            var jsonDocuments = GetJsonDocuments(sourceReplicationInformation.LastDocumentEtag);
            if (jsonDocuments == null || jsonDocuments.Count == 0)
                return null;
            if (TryReplicationDocuments(destination, jsonDocuments) == false)// failed to replicate, start error handling strategy
            {
                if (IsFirstFailue(destination))
                {
                    log.InfoFormat(
                        "This is the first failure for {0}, assuming transinet failure and trying again",
                        destination);
                    if (TryReplicationDocuments(destination, jsonDocuments))// success on second faile
                        return true;
                }
                IncrementFailureCount(destination);
                return false;
            }
            return true;
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

        private bool TryReplicationAttachments(string destination, JArray jsonAttachments)
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(destination + "/replication/replicateAttachments?from=" + docDb.Configuration.ServerUrl);
                request.UseDefaultCredentials = true;
                request.Credentials = CredentialCache.DefaultNetworkCredentials;
                request.Method = "POST";
                using (var stream = request.GetRequestStream())
                {
                    jsonAttachments.WriteTo(new BsonWriter(stream));
                    stream.Flush();
                }
                using (request.GetResponse())
                {
                    log.InfoFormat("Replicated {0} attachments to {1}", jsonAttachments.Count, destination);
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

        private bool TryReplicationDocuments(string destination, JArray jsonDocuments)
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(destination + "/replication/replicateDocs?from=" + docDb.Configuration.ServerUrl);
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
                    log.InfoFormat("Replicated {0} documents to {1}", jsonDocuments.Count, destination);
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
					jsonDocuments = new JArray(actions.Documents.GetDocumentsAfter(etag)
                        .Where(x => x.Key.StartsWith("Raven/") == false) // don't replicate system docs
                        .Where(x => x.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) == instanceId) // only replicate documents created on this instance
                        .Where(x=> x.Metadata[ReplicationConstants.RavenReplicationConflict] == null) // don't replicate conflicted documents, that just propgate the conflict
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

        private JArray GetAttachments(Guid etag)
        {
            JArray jsonDocuments = null;
            try
            {
                var instanceId = docDb.TransactionalStorage.Id.ToString();
                docDb.TransactionalStorage.Batch(actions =>
                {
                    jsonDocuments = new JArray(actions.Attachments.GetAttachmentsAfter(etag)
                        .Where(x => x.Key.StartsWith("Raven/") == false) // don't replicate system docs
                        .Where(x => x.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) == instanceId) // only replicate documents created on this instance
                        .Where(x => x.Metadata[ReplicationConstants.RavenReplicationConflict] == null) // don't replicate conflicted documents, that just propgate the conflict
                        .Take(100)
                        .Select(x => new JObject
                        {
                            {"@metadata", x.Metadata},
                            {"@id", x.Key},
                            {"@etag", x.Etag.ToByteArray()},
                            {"data", actions.Attachments.GetAttachment(x.Key).Data}
                        }));
                });
            }
            catch (Exception e)
            {
                log.Warn("Could not get documents to replicate after: " + etag, e);
            }
            return jsonDocuments;
        }

        private SourceReplicationInformation GetLastReplicatedEtagFrom(string destination)
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
                    var etagFromServer = (SourceReplicationInformation)new JsonSerializer().Deserialize(new StreamReader(stream), typeof(SourceReplicationInformation));
                    return etagFromServer;
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
