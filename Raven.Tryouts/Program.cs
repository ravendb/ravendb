using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Issues;
using Raven.Tests.Notifications;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			var etag = Etag.Parse("01000000-0000-0001-0000-000001C9FEE9");
			var incrementBy = etag.IncrementBy(int.MaxValue);
			Console.WriteLine(incrementBy.ToString());
		}
	}


	
}