using System.Net.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.IO;
using System.Net;

namespace Raven.Backup
{
    using Abstractions.Connection;

    public class DatabaseBackupOperation : AbstractBackupOperation
    {
        private DocumentStore store;

        public DatabaseBackupOperation(BackupParameters parameters)
            : base(parameters)
        {
        }

        public override bool InitBackup()
        {
            parameters.ServerUrl = parameters.ServerUrl.TrimEnd('/');
            try //precaution - to show error properly just in case
            {
                var serverUri = new Uri(parameters.ServerUrl);
                if ((String.IsNullOrWhiteSpace(serverUri.PathAndQuery) || serverUri.PathAndQuery.Equals("/")) &&
                    String.IsNullOrWhiteSpace(parameters.Database))
                    parameters.Database = Constants.SystemDatabase;

                var serverHostname = serverUri.Scheme + Uri.SchemeDelimiter + serverUri.Host + ":" + serverUri.Port;

                store = new DocumentStore { Url = serverHostname, DefaultDatabase = parameters.Database, ApiKey = parameters.ApiKey, Credentials = parameters.Credentials };
                store.Initialize();
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc);
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
               BackupLocation = parameters.BackupPath
            };

           
            var json = RavenJObject.FromObject(backupRequest).ToString();

            var url = "/admin/backup";
            if (parameters.Incremental)
                url += "?incremental=true";
            try
            {
                using (var req = CreateRequest(url, HttpMethods.Post))
                {
                    req.WriteAsync(json).Wait();

                    Console.WriteLine("Sending json {0} to {1}", json, parameters.ServerUrl);

                    var response = req.ReadResponseJson();
                    Console.WriteLine(response);
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc);
                return false;
            }

            return true;
        }

        protected override HttpJsonRequest CreateRequest(string url, HttpMethod method)
        {
            var uriString = parameters.ServerUrl;
            if (string.IsNullOrWhiteSpace(parameters.Database) == false && !parameters.Database.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
            {
                uriString += "/databases/" + parameters.Database;
            }

            uriString += url;

            return store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, uriString, method, new OperationCredentials(parameters.ApiKey, parameters.Credentials), store.Conventions, timeout: parameters.Timeout.HasValue ? TimeSpan.FromMilliseconds(parameters.Timeout.Value) : (TimeSpan?)null));
        }

        public override BackupStatus GetStatusDoc()
        {
            using (var req = CreateRequest("/docs/" + BackupStatus.RavenBackupStatusDocumentKey, HttpMethods.Get))
            {
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
                        throw new IOException("Network error", ex);
                    }
                    if (res.StatusCode == HttpStatusCode.NotFound)
                    {
                        return null;
                    }
                }

                return null;
            }
        }

        public override void Dispose()
        {
            var _store = store;
            if (_store != null)
                _store.Dispose();
        }
    }
}
