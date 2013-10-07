//-----------------------------------------------------------------------
// <copyright file="DynamicQueryMapping.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class DynamicQueryMapping
	{
		[Fact]
		public void CanExtractTermsFromRangedQuery()
		{
			Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }),
														 "Term:[0 TO 10]", null);

			Assert.Equal("Term", mapping.Items[0].From);
		}


		[Fact]
		public void CanExtractTermsFromEqualityQuery()
		{
			Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }),
														 "Term:Whatever", null);

			Assert.Equal("Term", mapping.Items[0].From);
		}


		[Fact]
		public void CanExtractMultipleTermsQuery()
		{
			Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }),
														 "Term:Whatever OR Term2:[0 TO 10]", null);


			Assert.Equal(2, mapping.Items.Length);

			Assert.True(mapping.Items.Any(x => x.From == "Term"));

			Assert.True(mapping.Items.Any(x => x.From == "Term2"));
		}


		[Fact]
		public void CanExtractTermsFromComplexQuery()
		{
			Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }),
														 "+(Term:bar Term2:baz) +Term3:foo -Term4:rob", null);

			Assert.Equal(4, mapping.Items.Length);

			Assert.True(mapping.Items.Any(x => x.From == "Term"));

			Assert.True(mapping.Items.Any(x => x.From == "Term2"));

			Assert.True(mapping.Items.Any(x => x.From == "Term3"));

			Assert.True(mapping.Items.Any(x => x.From == "Term4"));
		}


		[Fact]
		public void CanExtractMultipleNestedTermsQuery()
		{
			Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }),
														 "Term:Whatever OR (Term2:Whatever AND Term3:Whatever)", null);

			Assert.Equal(3, mapping.Items.Length);

			Assert.True(mapping.Items.Any(x => x.From == "Term"));

			Assert.True(mapping.Items.Any(x => x.From == "Term2"));

			Assert.True(mapping.Items.Any(x => x.From == "Term3"));
		}


		[Fact]
		public void CreateDefinitionSupportsSimpleProperties()
		{
			var mapping = new Database.Data.DynamicQueryMapping

			{
				Items = new[]
				{
					new DynamicQueryMappingItem
					{
						From = "Name",
						To = "Name"
					}
				}
			};


			IndexDefinition definition = mapping.CreateIndexDefinition();

			Assert.Equal("from doc in docs\r\nselect new { Name = doc.Name }", definition.Map);
		}


		[Fact]
		public void CreateDefinitionSupportsArrayProperties()
		{
			var mapping = new Database.Data.DynamicQueryMapping

			{
				Items = new[]
				{
					new DynamicQueryMappingItem
					{
						From = "Tags,Name",
						To = "docTagsName"
					}
				}
			};


			IndexDefinition definition = mapping.CreateIndexDefinition();

			Assert.Equal(
        "from doc in docs\r\nselect new { docTagsName = (from docTagsItem in ((IEnumerable<dynamic>)doc.Tags).DefaultIfEmpty() select docTagsItem.Name).ToArray() }",
				definition.Map);
		}


		[Fact]
		public void CreateDefinitionSupportsNestedProperties()
		{
			var mapping = new Database.Data.DynamicQueryMapping

			{
				Items = new[]
				{
					new DynamicQueryMappingItem
					{
						From = "User.Name",
						To = "UserName"
					}
				}
			};


			IndexDefinition definition = mapping.CreateIndexDefinition();

			Assert.Equal("from doc in docs\r\nselect new { UserName = doc.User.Name }", definition.Map);
		}
	}
}
