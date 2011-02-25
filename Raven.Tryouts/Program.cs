using System;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Document;
using Raven.Tests.Stress;
using Xunit;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			while (true)
			{
				using (var x = new DocumentStoreServerTests())
					x.Can_create_index_using_linq_from_client();
				Console.WriteLine(DateTime.Now);
			}
		}
	}
}
