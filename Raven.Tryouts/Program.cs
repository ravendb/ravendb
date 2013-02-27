using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			Console.WriteLine(IndexingUtil.MapBucket("users/123"));
			Console.WriteLine(IndexingUtil.MapBucket("users/1252"));
		}
	}

}