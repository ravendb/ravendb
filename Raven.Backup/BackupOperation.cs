using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Net;
using System.Threading;

namespace Raven.Backup
{
    using Raven.Abstractions.Connection;

    public class BackupOperation : IDisposable
    {
        private DocumentStore store;
        public string ServerUrl { get; set; }
        public string BackupPath { get; set; }
        public bool NoWait { get; set; }
        public NetworkCredential Credentials { get; set; }

        public bool Incremental { get; set; }
        public int? Timeout { get; set; }
        public string ApiKey { get; set; }
        public string Database { get; set; }

        public BackupOperation()
        {
            Credentials = CredentialCache.DefaultNetworkCredentials;
        }

        public bool InitBackup()
        {
            ServerUrl = ServerUrl.TrimEnd('/');
            try //precaution - to show error properly just in case
            {
                var serverUri = new Uri(ServerUrl);
                if ((String.IsNullOrWhiteSpace(serverUri.PathAndQuery) || serverUri.PathAndQuery.Equals("/")) &&
                    String.IsNullOrWhiteSpace(Database))
                    Database = Constants.SystemDatabase;

                var serverHostname = serverUri.Scheme + Uri.SchemeDelimiter + serverUri.Host + ":" + serverUri.Port;

                store = new DocumentStore { Url = serverHostname, DefaultDatabase = Database, ApiKey = ApiKey };
                store.Initialize();
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
                try
                {
                    store.Dispose();
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception) { }
                return false;
            }


            var backupRequest = new
            {
                BackupLocation = BackupPath.Replace("\\", "\\\\"),
                DatabaseDocument = new DatabaseDocument { Id = Database }
            };

            var json = RavenJObject.FromObject(backupRequest).ToString();

            var url = "/admin/backup";
            if (Incremental)
                url += "?incremental=true";
            try
            {
                var req = CreateRequest(url, "POST");

                req.WriteAsync(json).Wait();

                Console.WriteLine("Sending json {0} to {1}", json, ServerUrl);

                var response = req.ReadResponseJson();
                Console.WriteLine(response);
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
                return false;
            }

            return true;
        }

        private HttpJsonRequest CreateRequest(string url, string method)
        {
            var uriString = ServerUrl;
            if (string.IsNullOrWhiteSpace(Database) == false && !Database.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
            {
                uriString += "/databases/" + Database;
            }

            uriString += url;

            var req = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, uriString, method, new OperationCredentials(ApiKey, CredentialCache.DefaultCredentials), store.Conventions));
            Console.WriteLine("Request url - " + uriString);
            if (Timeout.HasValue)
            {
                req.Timeout = TimeSpan.FromMilliseconds(Timeout.Value);
            }

            return req;
        }

        public void WaitForBackup()
        {
            BackupStatus status = null;
            var messagesSeenSoFar = new HashSet<BackupStatus.BackupMessage>();

            while (status == null)
            {
                Thread.Sleep(100); // Allow the server to process the request
                status = GetStatusDoc();
            }

            if (NoWait)
            {
                Console.WriteLine("Backup operation has started, status is logged at Raven/Backup/Status");
                return;
            }

            while (status.IsRunning)
            {
                // Write out the messages as we poll for them, don't wait until the end, this allows "live" updates
                foreach (var msg in status.Messages)
                {
                    if (messagesSeenSoFar.Add(msg))
                    {
                        Console.WriteLine("[{0}] {1}", msg.Timestamp, msg.Message);
                    }
                }

                Thread.Sleep(1000);
                status = GetStatusDoc();
            }

            // After we've know it's finished, write out any remaining messages
            foreach (var msg in status.Messages)
            {
                if (messagesSeenSoFar.Add(msg))
                {
                    Console.WriteLine("[{0}] {1}", msg.Timestamp, msg.Message);
                }
            }
        }

        public BackupStatus GetStatusDoc()
        {
            var req = CreateRequest("/docs/" + BackupStatus.RavenBackupStatusDocumentKey, "GET");

            try
            {
                var json = (RavenJObject)req.ReadResponseJson();
                return json.Deserialize<BackupStatus>(store.Conventions);
            }
            catch (WebException ex)
            {
                var res = ex.Response as HttpWebResponse;
                if (res == null)
                {
                    throw new Exception("Network error");
                }
                if (res.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }
            }

            return null;
        }

        public void Dispose()
        {
            var _store = store;
            if (_store != null)
                _store.Dispose();
        }
    }
}