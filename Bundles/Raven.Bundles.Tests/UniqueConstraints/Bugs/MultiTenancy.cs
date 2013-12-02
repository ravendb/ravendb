using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Bundles.UniqueConstraints;
using Raven.Client.Embedded;
using Raven.Client.UniqueConstraints;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.UniqueConstraints.Bugs
{
	public class MultiTenancy : IDisposable
	{
		public MultiTenancy()
		{
			var listener = new UniqueConstraintsStoreListener(new CustomUniqueConstraintsTypeDictionary());
			this.DocumentStore = InitializeDocumentStore(listener);
		}

		private EmbeddableDocumentStore DocumentStore { get; set; }

		[Fact]
		public void Round_Trip_Includes_Expected_Metadata()
		{
			var original = new User() { Email = "foo@bar.com", Username = "Foo" };
			//var user2 = new User() { Email = "foo@bar.com", Username = "Foo Bar" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(original);
				//session.Store(user2);

				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var roundTripped = session.LoadByUniqueConstraint<User>(u => u.Email, original.Email);
				var metadata = session.Advanced.GetMetadataFor(roundTripped);
				var constraints = metadata.Value<RavenJArray>("Ensure-Unique-Constraints");
				
				Assert.Equal(2, constraints.Length);
			}
		}

		private EmbeddableDocumentStore InitializeDocumentStore(UniqueConstraintsStoreListener listener)
		{
			EmbeddableDocumentStore documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				UseEmbeddedHttpServer = true,
				Configuration =
				{
					Port = 8079
				}
			};

			documentStore.Configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(UniqueConstraintsPutTrigger).Assembly));
			documentStore.RegisterListener(listener);

			documentStore.Initialize();

			return documentStore;
		}

		class User
		{
			public string Id { get; set; }

			// Email is optionally unique based on tenant configuration
			public string Email { get; set; }

			[UniqueConstraint(CaseInsensitive=true)]
			public string Username { get; set; }
		}

		class CustomUniqueConstraintsTypeDictionary : UniqueConstraintsTypeDictionary
		{
			protected override System.Reflection.PropertyInfo[] GetUniqueProperties(Type type)
			{
				var props = base.GetUniqueProperties(type);

				if (type == typeof(User))
				{
					props = props.Union(new PropertyInfo[] { typeof(User).GetProperty("Email") }).ToArray();
				}

				return props;
			}
		}

		#region IDisposable

		private bool disposed = false;

		public void Dispose()
		{
			if (!this.disposed)
			{
				this.disposed = true;
				OnDispose(true);
				GC.SuppressFinalize(this);
			}
		}

		~MultiTenancy()
		{
			OnDispose(false);
		}

		protected virtual void OnDispose(bool disposing)
		{
			if (disposing)
			{
				this.DocumentStore.Dispose();
			}
		}

		#endregion
	}
}
