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
			for (int i = 0; i < 15; i++)
			{
				var memoryStream = new MemoryStream(File.ReadAllBytes(@"c:\work\test2.data"));
				var sp = Stopwatch.StartNew();
				JToken.ReadFrom(new BsonReader(memoryStream));
				Console.WriteLine(sp.ElapsedMilliseconds);
			}
		}
	}
}
