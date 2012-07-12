using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Raven.Abstractions;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Backup
{
	public class BackupOperation
	{
		public string ServerUrl { get; set; }
		public string BackupPath { get; set; }
		public bool NoWait { get; set; }

		public bool Incremental { get; set; }

		public bool InitBackup()
		{
			ServerUrl = ServerUrl.TrimEnd('/');

			var json = @"{ ""BackupLocation"": """ + BackupPath.Replace("\\", "\\\\") + @""" }";

			var uriString = ServerUrl + "/admin/backup";
			if (Incremental)
				uriString += "?incremental=true";
			var req = WebRequest.Create(uriString);
			req.Method = "POST";
			req.UseDefaultCredentials = true;
			req.PreAuthenticate = true;
			req.Credentials = CredentialCache.DefaultCredentials;

			using (var streamWriter = new StreamWriter(req.GetRequestStream()))
			{
				streamWriter.WriteLine(json);
				streamWriter.Flush();
			}

			try
			{
				Console.WriteLine("Sending json {0} to {1}", json, ServerUrl);

				using (var resp = req.GetResponse())
				using (var reader = new StreamReader(resp.GetResponseStream()))
				{
					var response = reader.ReadToEnd();
					Console.WriteLine(response);
				}
			}
			catch (WebException we)
			{
				var response = we.Response as HttpWebResponse;
				if(response == null)
				{
					Console.WriteLine(we.Message);
					return false;
				}
				Console.WriteLine(response.StatusCode + " " + response.StatusDescription);
				using(var reader = new StreamReader(response.GetResponseStream()))
				{
					Console.WriteLine(reader.ReadToEnd());
					return false;
				}
			}
			catch (Exception exc)
			{
				Console.WriteLine(exc.Message);
				return false;
			}

			return true;
		}

		public void WaitForBackup()
		{
			JObject doc = null;

			while (doc == null)
			{
				Thread.Sleep(100); // Allow the server to process the request

				doc = GetStatusDoc();
			}

			if (NoWait)
			{
				Console.WriteLine("Backup operation has started, status is logged at Raven/Backup/Status");
				return;
			}

			while (doc.Value<bool>("IsRunning"))
			{
				Thread.Sleep(1000);

				doc = GetStatusDoc();
			}

			var res = from msg in doc["Messages"]
					  select new
								{
									Message = msg.Value<string>("Message"),
									Timestamp = msg.Value<DateTime>("Timestamp"),
									Severity = msg.Value<string>("Severity")
								};

			foreach (var msg in res)
			{
				Console.WriteLine(string.Format("[{0}] {1}", msg.Timestamp, msg.Message));
			}
		}

		public JObject GetStatusDoc()
		{
			var req = WebRequest.Create(ServerUrl + "/docs/Raven/Backup/Status");
			req.Method = "GET";
			req.UseDefaultCredentials = true;
			req.PreAuthenticate = true;
			req.Credentials = CredentialCache.DefaultCredentials;

			try
			{
				JObject ret;
				using (var resp = req.GetResponse())
				using (var reader = new StreamReader(resp.GetResponseStream()))
				{
					var response = reader.ReadToEnd();
					ret = JObject.Parse(response);
				}
				return ret;
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
	}
}
