// -----------------------------------------------------------------------
//  <copyright file="NonHttpBackupRestore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class NonHttpBackupRestore : RavenTest
	{
		[Fact]
		public void CanImportFromDumpFile()
		{
			var options = new SmugglerOptions { BackupPath = Path.GetTempFileName() };
			using (var store = NewDocumentStoreWithData())
			{
				var dumper = new DataDumper(store.DocumentDatabase, options);
				dumper.ExportData(options);
			}

			using (var store = NewDocumentStore())
			{
				var dumper = new DataDumper(store.DocumentDatabase, options);
				dumper.ImportData(options);

				using (var session = store.OpenSession())
				{
					// Person imported.
					Assert.Equal(1, session.Query<Person>().Customize(x => x.WaitForNonStaleResults()).Take(5).Count());

					// Attachment imported.
					var attachment = store.DatabaseCommands.GetAttachment("Attachments/1");
					var data = ReadFully(attachment.Data());
					Assert.Equal(new byte[] { 1, 2, 3 }, data);
				}
			}
		}

		[Fact]
		public void ImportReplacesAnExistingDatabase()
		{
			var options = new SmugglerOptions { BackupPath = Path.GetTempFileName() };

			using (var store = NewDocumentStoreWithData())
			{
				var dumper = new DataDumper(store.DocumentDatabase,options);
				dumper.ExportData(options);

				using (var session = store.OpenSession())
				{
					var person = session.Load<Person>(1);
					person.Name = "Sean Kearon";

					session.Store(new Person { Name = "Gillian" });

					store.DatabaseCommands.DeleteAttachment("Attachments/1", null);

					store.DatabaseCommands.PutAttachment(
						"Attachments/2",
						null,
						new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 }),
						new RavenJObject { { "Description", "This is another attachment." } });

					session.SaveChanges();
				}

				new DataDumper(store.DocumentDatabase,options).ImportData(options);
				using (var session = store.OpenSession())
				{
					// Original attachment has been restored.
					Assert.NotNull(store.DatabaseCommands.GetAttachment("Attachments/1"));

					// The newly added attachment is still there.
					Assert.NotNull(store.DatabaseCommands.GetAttachment("Attachments/2"));

					// Original person has been restored.
					Assert.NotNull(session.Query<Person, PeopleByName>().Customize(x => x.WaitForNonStaleResults()).Single(x => x.Name == "Sean"));

					// The newly added person has not been removed.
					Assert.True(session.Query<Person, PeopleByName>().Customize(x => x.WaitForNonStaleResults()).Any(x => x.Name == "Gillian"));
				}
			}
		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			base.CreateDefaultIndexes(documentStore);
			new PeopleByName().Execute(documentStore);
		}

		protected byte[] ReadFully(Stream input)
		{
			var buffer = new byte[16 * 1024];
			using (var ms = new MemoryStream())
			{
				int read;
				while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
				{
					ms.Write(buffer, 0, read);
				}
				return ms.ToArray();
			}
		}

		private EmbeddableDocumentStore NewDocumentStoreWithData()
		{
			var store = NewDocumentStore();

			using (var session = store.OpenSession())
			{
				session.Store(new Person { Name = "Sean" });
				session.SaveChanges();

				store.DatabaseCommands.PutAttachment(
					"Attachments/1",
					null,
					new MemoryStream(new byte[] { 1, 2, 3 }),
					new RavenJObject { { "Description", "This is an attachment." } });
			}

			using (var session = store.OpenSession())
			{
				// Ensure the index is built.
				var people = session.Query<Person, PeopleByName>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.Name == "Sean")
					.ToArray();
				Assert.NotEmpty(people);
			}

			return store;
		}

		public class PeopleByName : AbstractIndexCreationTask<Person>
		{
			public PeopleByName()
			{
				Map = (persons => from person in persons
								  select new
								  {
									  person.Name,
								  });
			}
		}

		public class Person
		{
			public string Id;
			public string Name;
		}
	}
}
