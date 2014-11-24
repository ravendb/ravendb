using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Samina : RavenTest
	{

		public class Property
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
			public int BedroomCount { get; set; }
		}

		public class Catalog
		{
			public Guid Id { get; set; }
			public Guid PropertyId { get; set; }
			public string Type { get; set; }
		}

		[Fact]
		public void Can_search_with_filters()
		{
			Property property = new Property { Id = Guid.NewGuid(), Name = "Property Name", BedroomCount = 3 };
			Catalog catalog = new Catalog() { Id = Guid.NewGuid(), Type = "Waterfront", PropertyId = property.Id };

			using(var store = NewDocumentStore())
			using(var _session = store.OpenSession())
			{

				_session.Store(property);
				_session.Store(catalog);
				_session.SaveChanges();

                var catalogs = _session.Advanced.DocumentQuery<Catalog>().WhereEquals("Type", "Waterfront").Select(c => c.PropertyId);
                var properties = _session.Advanced.DocumentQuery<Property>();
				properties.OpenSubclause();
				var first = true;
				foreach (var guid in catalogs)
				{
					if (first == false)
						properties.OrElse(); 
					properties.WhereEquals("__document_id", guid);
					first = false;
				}
				properties.CloseSubclause();
				var refinedProperties = properties.AndAlso().WhereGreaterThanOrEqual("BedroomCount", "2").Select(p => p.Id);

				Assert.NotEqual(0, refinedProperties.Count());
			}
		}
	}
}