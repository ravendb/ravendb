using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace Uploader
{
	class Program
	{
		static void Main(string[] args)
		{
			if(args.Length < 2)
			{
				Console.WriteLine("Usage: uploader.exe [raven url] [directory]");
				return;
			}

			long totalTimes = 0;
			int totalDocs = 0;
			int totalRequests = 0;

			var ravenUrl = args[0];
			foreach (var directory in args.Skip(1))
			{
				foreach (var file in Directory.GetFiles(directory, "posts*.json"))
				{

					var sp = Stopwatch.StartNew();
					HttpWebResponse webResponse;
					while (true)
					{
						totalDocs+=1024;
						totalRequests++;
						var httpWebRequest = (HttpWebRequest)WebRequest.Create(new Uri(new Uri(ravenUrl), "bulk_docs"));
						httpWebRequest.Method = "POST";
						using (var requestStream = httpWebRequest.GetRequestStream())
						{
							var readAllBytes = File.ReadAllBytes(file);
							requestStream.Write(readAllBytes, 0, readAllBytes.Length);
						}
						try
						{
							webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
							webResponse.Close();
							break;
						}
						catch (WebException e)
						{
							Console.WriteLine(e.Message);
							Console.WriteLine("{0} - {1}", Path.GetFileName(file), sp.Elapsed);
							webResponse = e.Response as HttpWebResponse;
							if (webResponse != null)
							{
								Console.WriteLine("Http Status {0}", webResponse.StatusCode);
								Console.WriteLine(new StreamReader(webResponse.GetResponseStream()).ReadToEnd());
								return;
							}
							return;
						}
					}
					var timeSpan = sp.Elapsed;
					totalTimes += sp.ElapsedMilliseconds;
					Console.WriteLine("{0} - {1} - {2} - {3:#,#} docs - {4:#,#} ms", Path.GetFileName(file), webResponse.StatusCode, timeSpan, totalDocs, totalTimes / totalRequests);
				}

			}
		}
	}
}
