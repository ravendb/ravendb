// -----------------------------------------------------------------------
//  <copyright file="Kushnir.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Kushnir : RavenTest
	{
		private static string alphabet = "abcdefghijklmnopqrstuvwxyz";
		
		[Fact]
		public void SortOnMetadata()
		{
			using (var docStore = NewDocumentStore())
			{
				docStore.RegisterListener(new CreateDateMetadataConversion());
				new Foos_ByNameDateCreated().Execute(docStore);


				for (int i = 0; i < alphabet.Length; i++)
				{
					using (var session = docStore.OpenSession())
					{
						session.Store(new Foo { Name = alphabet[i].ToString(CultureInfo.InvariantCulture), Id = "Foos/" });
						session.SaveChanges();
					}
				}

				WaitForIndexing(docStore);

				using (var session = docStore.OpenSession())
				{
					var ascending = session.Query<Foo, Foos_ByNameDateCreated>()
						   .Where(a => a.Name.StartsWith(string.Empty))
						   .OrderBy(a => a.DateCreated)
						   .ToList();
					var descending = session.Query<Foo, Foos_ByNameDateCreated>()
						   .Where(a => a.Name.StartsWith(string.Empty))
						   .OrderByDescending(a => a.DateCreated)
						   .ToList();


					Assert.Equal(alphabet, ascending.Select(a => a.Name).Aggregate((a, b) => a + b));
					Assert.Equal(new string(alphabet.Reverse().ToArray()), @descending.Select(a => a.Name).Aggregate((a, b) => a + b));
				}
			}
		}

		public interface ITimeStamped { DateTime DateCreated { get; set; } }

		public class Foo : ITimeStamped
		{
			public string Id { get; set; }
			public string Name { get; set; }

			public DateTime DateCreated { get; set; }
		}

		public class CreateDateMetadataConversion : IDocumentConversionListener
		{
			private int count;
			public void EntityToDocument(string key, object entity, RavenJObject document, RavenJObject metadata)
			{
				if (entity is ITimeStamped)
				{
					document.Remove("DateCreated");
					if (metadata["DateCreated"] == null)
					{
						metadata["DateCreated"] = DateTime.Today.AddDays(count++).ToString("o");
					}
				}
			}

			public void DocumentToEntity(string key, object entity, RavenJObject document, RavenJObject metadata)
			{
				var timestamped = entity as ITimeStamped;
				if (timestamped != null && metadata.ContainsKey("DateCreated"))
				{
					DateTime createDate = DateTime.Parse(metadata["DateCreated"].ToString());
					DateTime.SpecifyKind(createDate, DateTimeKind.Utc);
					timestamped.DateCreated = createDate;
				}
			}
		}

		public class Foos_ByNameDateCreated : AbstractIndexCreationTask<Foo>
		{
			public Foos_ByNameDateCreated()
			{
				Map = foos => foos.Select(a => new { a.Name, DateCreated = MetadataFor(a).Value<DateTime>("DateCreated") });
			}
		}
	}
}