using System;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Tests.Bugs.LiveProjections.Entities;
using Task = System.Threading.Tasks.Task;

namespace Raven.Tryouts
{
    public class Program
	{
	    private static void Main()
	    {
		    using (var store = new DocumentStore
		    {
			    DefaultDatabase =  "mystikit",
				Url = "https://db1.mystikit.com/",
				Credentials = new NetworkCredential("basketeer", "B@$keteer", "db1-mystikit"),
				
		    })
		    {
			    store.Initialize();
			    var stopEvent = new ManualResetEventSlim();

			    var queryTask = Task.Run(() =>
			    {
				    do
				    {
					    var result = store
						    .DatabaseCommands
						    .Query(Constants.DocumentsByEntityNameIndex, new IndexQuery {ShowTimings = true});

						Console.WriteLine("-------------------------------------------------------------------------------------");
						Console.WriteLine("Total query duration : {0} ms, total duration server-side: {1}ms, Total Request Size: {2} bytes", 
							result.DurationMilliseconds, result.TimingsInMilliseconds.Sum(x => x.Value), result.ResultSize);
						Console.WriteLine("Breaking of server-side timings:");
						foreach(var timing in result.TimingsInMilliseconds)
							Console.WriteLine("{0} -> {1} ms",timing.Key, timing.Value);
						Console.WriteLine("-------------------------------------------------------------------------------------");
					} while (stopEvent.Wait(100) == false);
			    });

			    Console.ReadLine();
				stopEvent.Set();
		    }
	    }
	}	
}