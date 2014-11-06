using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.DiskIO;
using Raven.Json.Linq;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080",
			}.Initialize())
			{
				
				documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
				{
					Id = "MyDb",
					Settings =
					{
						{"Raven/RunInMemory", "true"}
					}
				});

				// run your code

				documentStore.DatabaseCommands.GlobalAdmin.DeleteDatabase("MyDb");
			}
		}
	}


	
}