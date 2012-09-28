extern alias database;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.UniqueConstraints;
using Raven.Tests;
using Xunit;

namespace Raven.Bundles.Tests.UniqueConstraints.Bugs
{
	public class TroyMapReduce : RavenTest
	{
		private Guid _appKey;
		private Guid _privateKey;

		protected override void ModifyConfiguration(database::Raven.Database.Config.RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(Bundles.UniqueConstraints.UniqueConstraintsPutTrigger).Assembly));
		}

		protected override void ModifyStore(Client.Document.DocumentStore store)
		{
			store.Conventions.TransformTypeTagNameToDocumentKeyPrefix = tag => tag;
			store.Conventions.FindTypeTagName = type => type.Name;
			store.RegisterListener(new UniqueConstraintsStoreListener());
		}

		[Fact]
		public void UsingEmbedded()
		{
			using (var store = NewDocumentStore())
			{
				new LogEntryCountByDate().Execute(store);
				using (var session = store.OpenSession())
				{
					SetKeys();
					var application = PopulateApplicaiton();
					var entries = PopulateLogEntries();

					session.Store(application);
					entries.ForEach(session.Store);
					session.SaveChanges();

					var query = session.Query<LogEntryCountByDate.SearchResult>("LogEntry/CountByDate")
						//.Where( x => x.EntryDate == DateTime.Today )
					  .Customize(x => x.WaitForNonStaleResultsAsOfNow());

					Assert.Equal(4, query.Count());
					Assert.Equal(1, query.First().EntryCount);
				}
			}
		}

		[Fact]
		public void UsingRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new LogEntryCountByDate().Execute(store);
				using (var session = store.OpenSession())
				{
					SetKeys();
					var application = PopulateApplicaiton();
					var entries = PopulateLogEntries();

					session.Store(application);
					entries.ForEach(session.Store);
					session.SaveChanges();

					var query = session.Query<LogEntryCountByDate.SearchResult>("LogEntry/CountByDate")
						//.Where( x => x.EntryDate == DateTime.Today )
					  .Customize(x => x.WaitForNonStaleResultsAsOfNow());

					Assert.Equal(4, query.Count());
					Assert.Equal(1, query.First().EntryCount);
				}
			}
		}

		private void SetKeys()
		{
			_appKey = Guid.Parse("7378e8ae-ddcd-460d-95f7-e89775bb0540");
			_privateKey = Guid.Parse("0bf04dd0-f8a2-4921-a441-1035fcc1d106");
		}

		private Application PopulateApplicaiton()
		{
			return new Application
			{
				ApplicationKey = _appKey,
				PrivateKey = _privateKey,
				Created = DateTimeOffset.UtcNow,
				Name = "Test Application",
				TimeZone = "Easter Standard Time"
			};
		}

		private List<LogEntry> PopulateLogEntries()
		{
			return new List<LogEntry>
				{
					new LogEntry
						{
							CreatedUtc = DateTimeOffset.UtcNow.AddDays(-2),
							Created = DateTime.Now.AddDays(-2),
							ApplicationKey = _appKey,
							PrivateKey = _privateKey,
							Dictionary = new List<LogEntryDictionaryItem>
								{
									new LogEntryDictionaryItem {Key = "Test", Value = "Test"}
								}
						},
					new LogEntry
						{
							CreatedUtc = DateTimeOffset.UtcNow.AddDays(-1),
							Created = DateTime.Now.AddDays(-1),
							ApplicationKey = _appKey,
							PrivateKey = _privateKey,
							Dictionary = new List<LogEntryDictionaryItem>
								{
									new LogEntryDictionaryItem {Key = "Test", Value = "Test"}
								}
						},
					new LogEntry
						{
							CreatedUtc = DateTimeOffset.UtcNow,
							Created = DateTime.Now,
							ApplicationKey = _appKey,
							PrivateKey = _privateKey,
							Dictionary = new List<LogEntryDictionaryItem>
								{
									new LogEntryDictionaryItem {Key = "Test", Value = "Test"}
								}
						},
					new LogEntry
						{
							CreatedUtc = DateTimeOffset.UtcNow.AddDays(+1),
							Created = DateTime.Now.AddDays(+1),
							ApplicationKey = _appKey,
							PrivateKey = _privateKey,
							Dictionary = new List<LogEntryDictionaryItem>
								{
									new LogEntryDictionaryItem {Key = "Test", Value = "Test"}
								}
						}

				};


		}

		public class Application
		{
			public string Id { get; set; }
			public DateTimeOffset Created { get; set; }
			public string Name { get; set; }
			public Guid PrivateKey { get; set; }
			public Guid ApplicationKey { get; set; }
			public string TimeZone { get; set; }
		}

		public class LogEntry
		{
			public string Id { get; set; }
			public DateTime Created { get; set; }
			public DateTimeOffset CreatedUtc { get; set; }
			public Guid ApplicationKey { get; set; }
			public Guid PrivateKey { get; set; }
			public List<LogEntryDictionaryItem> Dictionary { get; set; }
			public LogEntry()
			{
				Dictionary = new List<LogEntryDictionaryItem>();
			}
		}

		public class LogEntryDictionaryItem
		{
			public string Key { get; set; }
			public object Value { get; set; }
		}


		public class LogEntryCountByDate : AbstractIndexCreationTask<LogEntry, LogEntryCountByDate.SearchResult>
		{

			public override string IndexName
			{
				get { return "LogEntry/CountByDate"; }
			}

			public class SearchResult
			{
				public int EntryCount { get; set; }
				public DateTime EntryDate { get; set; }
				public string PrivateKey { get; set; }
				public string ApplicationKey { get; set; }
			}

			public LogEntryCountByDate()
			{
				Map = logEntries =>
					  from logEntry in logEntries
					  select new
					  {
						  EntryCount = 1,
						  EntryDate = logEntry.Created.Date,
						  logEntry.ApplicationKey,
						  logEntry.PrivateKey
					  };

				Reduce =
				  logEntries =>
				  logEntries.GroupBy(x => new { x.ApplicationKey, x.PrivateKey, x.EntryDate }).Select(
					x => new { x.Key.ApplicationKey, x.Key.PrivateKey, x.Key.EntryDate, EntryCount = x.Sum(y => y.EntryCount) });
			}
		}
	}
}