using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Tests.Bugs.Identifiers;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MetadataBugs : IisExpressTestClient
	{
		public class DeviceStatusRecord
		{
			public int DeviceId { get; set; }
			public DateTimeOffset Timestamp { get; set; }
			public int StatusId { get; set; }
		}

		[IISExpressInstalledFact]
		public void CanHandleCaseSensitivityInProperties()
		{
			using (NewDocumentStore())
			{
				DoTest("mixedCase", "value", 30);
			}
		}

		[IISExpressInstalledFact]
		public void CanHandleStandardCasing()
		{
			using (NewDocumentStore())
			{
				DoTest("ProperCase", "value", 30);
			}
		}
		public class Sesion
		{

			public String Id { get; set; }
			public String Name { get; set; }
			public String Description { get; set; }
		}


		private static void DoTest(String propertyName, String propertyValue, int iterations)
		{
			using (var database = new DocumentStore {Url = "http://localhost:8084"})
			{
				database.Initialize();

				String id = "Session/" + DateTime.Now.Ticks;
				Console.WriteLine("Document ID: " + id);

				//create document with a metadata field
				using (var session = database.OpenSession())
				{

					var s = new Sesion {Id = id, Name = "Sesion " + id, Description = "Desc " + id};
					session.Store(s);
					var metadata = session.Advanced.GetMetadataFor(s);
					metadata[propertyName] = propertyValue;
					session.SaveChanges();

				}

				//overwrite metadata
				for (int i = 0; i < iterations; i++)
				{
					using (var session = database.OpenSession())
					{
						var s = session.Load<Sesion>(id);
						var metadata = session.Advanced.GetMetadataFor(s);
						metadata[propertyName] = propertyValue; // same property name, same value
						session.SaveChanges();
					}
				}

				//read metadata, and print it 
				using (var session = database.OpenSession())
				{
					Sesion s = session.Load<Sesion>(id);
					var metadata = session.Advanced.GetMetadataFor(s);
					Console.WriteLine(metadata);
					Assert.Equal(JTokenType.String, metadata[propertyName].Type);
				}
			}
		}
	}
}