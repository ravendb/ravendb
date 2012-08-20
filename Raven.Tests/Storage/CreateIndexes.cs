//-----------------------------------------------------------------------
// <copyright file="CreateIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Config;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class CreateIndexes : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public CreateIndexes()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Index_with_same_name_can_be_added_twice()
		{
			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});
		}

		[Fact]
		public void Can_add_index()
		{
			db.PutIndex("pagesByTitle",
			            new IndexDefinition
			            {
							Map = @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
			            });
			var indexNames = db.IndexDefinitionStorage.IndexNames.Where(x=>x.StartsWith("Raven") == false).ToArray();
			Assert.Equal(1, indexNames.Length);
			Assert.Equal("pagesByTitle", indexNames[0]);
		}

		[Fact]
		public void Index_names_should_be_sorted_alphabetically()
		{
			const string unimportantIndexMap = @"from doc in docs select new { doc };";
			db.PutIndex("zebra", new IndexDefinition { Map = unimportantIndexMap });
			db.PutIndex("alligator", new IndexDefinition { Map = unimportantIndexMap });
			db.PutIndex("monkey", new IndexDefinition { Map = unimportantIndexMap });

			var indexNames = db.IndexDefinitionStorage.IndexNames
				.Where(x => x.StartsWith("Raven") == false)
				.ToArray();

			Assert.Equal("alligator", indexNames[0]);
			Assert.Equal("monkey", indexNames[1]);
			Assert.Equal("zebra", indexNames[2]);
		}

		[Fact]
		public void Can_list_index_definition()
		{
			const string definition =
				@" 
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
			db.PutIndex("pagesByTitle", new IndexDefinition{Map = definition});
			var actualDefinition = db.IndexDefinitionStorage.GetIndexDefinition("pagesByTitle");
			Assert.Equal(definition, actualDefinition.Map);
		}
	}
}
