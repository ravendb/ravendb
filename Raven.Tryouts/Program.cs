using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;

namespace Raven.Tryouts
{
	public class Item2
	{
		public double Price;
	}
	public class Program
	{
		private static void Main()
		{
			var x= new DocumentStore
			{
				Url = "http://live-test.ravendb.net",
				DefaultDatabase = "maxim"
			}.Initialize();
			using (var s = x.OpenSession())
			{
				s.Query<Item2>().Where(a => a.Price == 100).ToList();
			}
		}

	}
}