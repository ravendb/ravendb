using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Raven.Backup
{
	class Program
	{
		static void Main(string[] args)
		{
			string url = null, dest = null;
			if (args.Length == 4)
			{
				url = (args[0] == "-url") ? args[1] : ((args[2] == "-url") ? args[3] : null);
				dest = (args[0] == "-dest") ? args[1] : ((args[2] == "-dest") ? args[3] : null);
			}
			else
			{
				Console.WriteLine("Syntax: Raven.Backup -url http://raven-server-url-here/ -dest full-file-Path");
			}
			
			if (string.IsNullOrWhiteSpace(url))
			{
				Console.WriteLine("Enter RavenDB server URL:");
				url = Console.ReadLine();
			}

			if (string.IsNullOrWhiteSpace(dest))
			{
				Console.WriteLine("Enter backup location:");
				dest = Console.ReadLine();
			}

			if (string.IsNullOrWhiteSpace(dest) || string.IsNullOrWhiteSpace(url))
				return;

			var json = @"{ ""BackupLocation"": """ + dest.Replace("\\", "\\\\") + @""" }";

			var req = WebRequest.Create(url + "/admin/backup");
			req.Method = "POST";
			req.UseDefaultCredentials = true;
			req.PreAuthenticate = true;
			req.Credentials = CredentialCache.DefaultCredentials;

			using (var streamWriter = new System.IO.StreamWriter(req.GetRequestStream()))
			{
				streamWriter.WriteLine(json);
				streamWriter.Flush();
			}

			try
			{
				Console.WriteLine(string.Format("Sending json {0} to {1}", json, url));

				using (var resp = req.GetResponse())
				using (var reader = new StreamReader(resp.GetResponseStream()))
				{
					var response = reader.ReadToEnd();
					Console.WriteLine(response);
				}

				Console.WriteLine("Backup completed successfully");
			}
			catch (Exception exc)
			{
				Console.WriteLine(exc.Message);
			}

			Console.ReadKey();
		}
	}
}
