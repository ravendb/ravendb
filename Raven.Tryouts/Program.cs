using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var webRequest = (HttpWebRequest)WebRequest.Create("http://localhost:8080/admin/compact?database=test");
			webRequest.Method = "POST";
			webRequest.UseDefaultCredentials = true;
			webRequest.Credentials = CredentialCache.DefaultCredentials;
			webRequest.ContentLength = 0;
			try
			{
				webRequest.GetResponse();
				Console.WriteLine("DONE");
			}
			catch(WebException we)
			{
				Console.WriteLine(new StreamReader((we.Response.GetResponseStream())).ReadToEnd());
			}
			Console.ReadLine();
		}
	}
}