using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using NLog;

namespace etobi.EmbeddedTest
{
	internal class Program
	{
		static void Main(string[] args)
		{
			var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			documentStore.DatabaseCommands.DeleteByIndex("Temp/AllDocs/By_metadata_Raven_Document_Revision_Status",
				new IndexQuery
				{
					Query = "_metadata_Raven_Document_Revision_Status:Historical"
				});
		}
	}
}