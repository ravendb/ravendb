using System;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Stress;
using Xunit;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			var sp = Stopwatch.StartNew();
			var readFrom = JObject.Load(new BsonReader(File.OpenRead(@"C:\Work\test.data")));
			Console.WriteLine(sp.ElapsedMilliseconds);
			sp.Reset();
			new JObject(readFrom);
			Console.WriteLine(sp.ElapsedMilliseconds);
		}
	}
}
