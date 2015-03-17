//-----------------------------------------------------------------------
// <copyright file="DynamicQueryMapping.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class DynamicQueryMapping : RavenTest
	{
		private DocumentDatabase CreateDocumentDatabase()
		{
			var configuration = new RavenConfiguration();
			configuration.DataDirectory = Path.Combine(NewDataPath(), "System");
			configuration.RunInMemory = configuration.DefaultStorageTypeName == InMemoryRavenConfiguration.VoronTypeName;
			return new DocumentDatabase(configuration, null);
		}

		[Fact]
		public void CanExtractTermsFromRangedQuery()
		{
			using (var database = CreateDocumentDatabase())
			{
				Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(database,
														 "Term:[0 TO 10]", null);

				Assert.Equal("Term", mapping.Items[0].From);
			}
		}

		[Fact]
		public void CanExtractTermsFromEqualityQuery()
		{
			using (var database = CreateDocumentDatabase())
			{
				Database.Data.DynamicQueryMapping mapping = Database.Data.DynamicQueryMapping.Create(database, "Term:Whatever", null);

				Assert.Equal("Term", mapping.Items[0].From);
			}
		}


		[Fact]
		public void CanExtractMultipleTermsQuery()
		{
			using (var database = CreateDocumentDatabase())
			{
				Database.Data.DynamicQueryMapping mapping = Database.Data.DynamicQueryMapping.Create(database, "Term:Whatever OR Term2:[0 TO 10]", null);


				Assert.Equal(2, mapping.Items.Length);

				Assert.True(mapping.Items.Any(x => x.From == "Term"));

				Assert.True(mapping.Items.Any(x => x.From == "Term2"));
			}
		}


		[Fact]
		public void CanExtractTermsFromComplexQuery()
		{
			using (var database = CreateDocumentDatabase())
			{
				Database.Data.DynamicQueryMapping mapping = Database.Data.DynamicQueryMapping.Create(database, "+(Term:bar Term2:baz) +Term3:foo -Term4:rob", null);

				Assert.Equal(4, mapping.Items.Length);

				Assert.True(mapping.Items.Any(x => x.From == "Term"));

				Assert.True(mapping.Items.Any(x => x.From == "Term2"));

				Assert.True(mapping.Items.Any(x => x.From == "Term3"));

				Assert.True(mapping.Items.Any(x => x.From == "Term4"));
			}
		}


		[Fact]
		public void CanExtractMultipleNestedTermsQuery()
		{
			using (var database = CreateDocumentDatabase())
			{
				Database.Data.DynamicQueryMapping mapping =
				Database.Data.DynamicQueryMapping.Create(database,
														 "Term:Whatever OR (Term2:Whatever AND Term3:Whatever)", null);

				Assert.Equal(3, mapping.Items.Length);

				Assert.True(mapping.Items.Any(x => x.From == "Term"));

				Assert.True(mapping.Items.Any(x => x.From == "Term2"));

				Assert.True(mapping.Items.Any(x => x.From == "Term3"));
			}
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

			Assert.Equal("from doc in docs\nselect new {\n\tName = doc.Name\n}", definition.Map);
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

			const string map = "from doc in docs\nselect new {\n\tdocTagsName = (\n\t\tfrom docTagsItem in ((IEnumerable<dynamic>)doc.Tags).DefaultIfEmpty()\n\t\tselect docTagsItem.Name).ToArray()\n}";

            Assert.Equal(map,definition.Map);
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

			Assert.Equal("from doc in docs\nselect new {\n\tUserName = doc.User.Name\n}", definition.Map);
		}
	}
}