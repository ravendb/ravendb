using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.MailingList
{
	public class TriggerCanAccessFullMetadataInQueryProjection : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public TriggerCanAccessFullMetadataInQueryProjection()
		{
			this.store = this.NewDocumentStore(catalog: (new TypeCatalog(typeof(FullMetadataReadTrigger))));
			new Index().Execute(this.store);
			this.db = this.store.DocumentDatabase;
		}

		public class Index : AbstractIndexCreationTask<Data>
		{
			public Index()
			{
				this.Map = docs => from doc in docs
								   select new
								   {
									   doc.Name
								   };
			}
		}

		[Fact]
		public void HaveFullMetadata()
		{
			Metadata value = new Metadata();
			using (IDocumentSession session = this.store.OpenSession())
			{
				Data obj = new Data();
				session.Store(obj);
				session.Advanced.GetMetadataFor(obj)[Metadata.Key] = RavenJToken.FromObject(value);
				session.SaveChanges();
			}

			using (IDocumentSession session = this.store.OpenSession())
			{
				session.Query<Data>().Customize(x => x.WaitForNonStaleResults()).Select(x => x.Name).ToList();
			}

			FullMetadataReadTrigger readTrigger = this.db.ReadTriggers.OfType<FullMetadataReadTrigger>().First();
			Assert.Equal(value.Id, readTrigger.Id);
		}

		public class FullMetadataReadTrigger : AbstractReadTrigger
		{
			public Guid Id { get; private set; }

			public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation readOperation, TransactionInformation transactionInformation)
			{
				if (readOperation == ReadOperation.Query)
				{
					RavenJObject rawValue = metadata[Metadata.Key] as RavenJObject;
					if (rawValue != null)
					{
						Metadata value = rawValue.JsonDeserialization<Metadata>();
						if (value != null)
						{
							this.Id = value.Id;
						}
					}
				}

				return ReadVetoResult.Allowed;
			}
		}

		public class Metadata
		{
			public const string Key = "X-Metadata";

			public Metadata()
			{
				this.Id = Guid.NewGuid();
			}

			public Guid Id { get; private set; }
		}

		public class Data
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
