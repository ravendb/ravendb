using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Backup
{
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

			store = new DocumentStore {Url = ServerUrl, DefaultDatabase = Database, ApiKey = ApiKey};
			store.Initialize();
			
			var json = @"{ ""BackupLocation"": """ + BackupPath.Replace("\\", "\\\\") + @""" }";

			var url = "/admin/backup";
			if (Incremental)
				url += "?incremental=true";
			var req = CreateRequest(url, "POST");
			
			req.Write(json);
			try
			{
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
			if (string.IsNullOrWhiteSpace(Database) == false)
			{
				uriString += "/databases/" + Database;
			}
			uriString += url;
			if (Incremental)
				uriString += "?incremental=true";
			var req = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, uriString, method, Credentials, store.Conventions));

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
