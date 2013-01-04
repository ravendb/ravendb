using Raven.Bundles.Versioning.Data;
using Raven.Client;
using Raven.Client.Bundles.Versioning;
using Raven.Client.Document;
using Raven.Server;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bundles.Versioning.Bugs
{
	public class MultiTenant : RavenTest
	{
		private IDocumentStore _documentStore;
		RavenDbServer ravenDbServer;
		private string _dbid = "Test";

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "Versioning";
		}

		public MultiTenant()
		{
			ravenDbServer = GetNewServer();
			_documentStore = new DocumentStore
			{
				Url = "http://localhost:8079",
			}.Initialize();
			_documentStore.DatabaseCommands.EnsureDatabaseExists(_dbid);


			// Configure the default versioning configuration
			using (var session = _documentStore.OpenSession(_dbid))
			{
				session.Store(new VersioningConfiguration
				{
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 5
				});
				session.SaveChanges();
			}

			// Add an entity, creating the first version.
			using (var session = _documentStore.OpenSession(_dbid))
			{
				session.Store(new Ball { Id = "balls/1", Color = "Red" });
				session.SaveChanges();
			}

			// Change the entity, creating the second version.
			using (var session = _documentStore.OpenSession(_dbid))
			{
				var ball = session.Load<Ball>("balls/1");
				ball.Color = "Blue";
				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			_documentStore.Dispose();
			ravenDbServer.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Test_That_Versioning_Documents_Were_Created()
		{
			// Passes

			var revisions = _documentStore.DatabaseCommands.ForDatabase(_dbid).StartsWith("balls/1/revisions/", null, 0, 10);
			Assert.Equal(2, revisions.Length);
		}

		[Fact]
		public void Test_GetRevisionsFor_Extension_Method()
		{
			// Should pass.  Fails.

			using (var session = _documentStore.OpenSession(_dbid))
			{
				var revisions = session.Advanced.GetRevisionsFor<Ball>("balls/1", 0, 10);

				Assert.Equal(2, revisions.Length);
				Assert.Equal("Red", revisions[0].Color);
				Assert.Equal("Blue", revisions[1].Color);
			}
		}

		[Fact]
		public void Test_GetRevisionIdsFor_Extension_Method()
		{
			// Should pass.  Fails.

			using (var session = _documentStore.OpenSession(_dbid))
			{
				var revisionIds = session.Advanced.GetRevisionIdsFor<Ball>("balls/1", 0, 10);

				Assert.Equal(2, revisionIds.Length);
				Assert.Equal("balls/1/revisions/1", revisionIds[0]);
				Assert.Equal("balls/1/revisions/2", revisionIds[1]);
			}
		}

		[Fact]
		public void Test_GetRevisionIdsFor_Fixed_Extension_Method()
		{
			// Passes

			using (var session = _documentStore.OpenSession(_dbid))
			{
				var revisionIds = session.Advanced.GetRevisionIdsFor<Ball>("balls/1", 0, 10);

				Assert.Equal(2, revisionIds.Length);
				Assert.Equal("balls/1/revisions/1", revisionIds[0]);
				Assert.Equal("balls/1/revisions/2", revisionIds[1]);
			}
		}

		[Fact]
		public void Test_GetRevisionsFor_Fixed_Extension_Method()
		{
			// Passes

			using (var session = _documentStore.OpenSession(_dbid))
			{
				var revisions = session.Advanced.GetRevisionsFor<Ball>("balls/1", 0, 10);

				Assert.Equal(2, revisions.Length);
				Assert.Equal("Red", revisions[0].Color);
				Assert.Equal("Blue", revisions[1].Color);
			}
		}


		public class Ball
		{
			public string Id { get; set; }
			public string Color { get; set; }
		}
	}
}