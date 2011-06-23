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

			var ravenUrl = args[0];
			foreach (var directory in args.Skip(1))
			{
				foreach (var file in Directory.GetFiles(directory, "*.json"))
				{

					var sp = Stopwatch.StartNew();
					HttpWebResponse webResponse;
					while (true)
					{

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
					Console.WriteLine("{0} - {1} - {2}", Path.GetFileName(file), timeSpan, webResponse.StatusCode);
				}

			}
		}
	}
}
