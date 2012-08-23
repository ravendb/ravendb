using System.ComponentModel.Composition.Hosting;

namespace Raven.Bundles.Tests.UniqueConstraints
{
	using System;
	using System.IO;

	using Raven.Bundles.UniqueConstraints;
	using Raven.Client.Embedded;
	using Raven.Client.UniqueConstraints;

	public abstract class UniqueConstraintsTest : IDisposable
	{
		protected UniqueConstraintsTest()
		{
			this.DocumentStore = new EmbeddableDocumentStore
				{
					RunInMemory = true, 
					UseEmbeddedHttpServer = true,
					Configuration =
						{
							Port = 8079
						}
				};
			this.DocumentStore.Configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(UniqueConstraintsPutTrigger).Assembly));
			this.DocumentStore.RegisterListener(new UniqueConstraintsStoreListener());

			this.DocumentStore.Initialize();
		}

		protected EmbeddableDocumentStore DocumentStore { get; set; }

		public void Dispose()
		{
			DocumentStore.Dispose();
		}
	}

	public class User
	{
		public string Id { get; set; }

		[UniqueConstraint]
		public string Email { get; set; }

		public string Name { get; set; }
	}
}
