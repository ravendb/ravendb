using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TailFeather.Client;
using Rachis.Tests;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 1000; i++) {
				using (var test = new TopologyChangesTests ()) {
					Console.WriteLine (i);
					test.New_node_can_be_added_even_if_it_is_down ();
				}
			}
			//var tailFeatherClient = new TailFeatherClient(new Uri("http://localhost:9078"));
			//int i =0;
			//while (true)
			//{
				//tailFeatherClient.Set("now-"+i, DateTime.Now);
				//Console.WriteLine(i++);
				//Console.ReadKey();
			//}
		}
	}

}
