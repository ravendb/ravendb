using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

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
				catch (Exception){ }
				return false;
			}


			var backupRequest = new
								{
									BackupLocation = BackupPath.Replace("\\", "\\\\")
								};

			var json = RavenJObject.FromObject(backupRequest).ToString();

			var url = "/admin/backup";
			if (Incremental)
				url += "?incremental=true";
			try
			{
				var req = CreateRequest(url, "POST");

				req.Write(json);

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
			if (string.IsNullOrWhiteSpace(Database) == false && !Database.Equals(Constants.SystemDatabase,StringComparison.OrdinalIgnoreCase))
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
				Thread.Sleep(1000);
				status = GetStatusDoc();
			}
			
			foreach (var msg in status.Messages)
			{
				Console.WriteLine("[{0}] {1}", msg.Timestamp, msg.Message);
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
