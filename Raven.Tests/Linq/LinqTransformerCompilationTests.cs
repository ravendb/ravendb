//-----------------------------------------------------------------------
// <copyright file="LinqTransformerCompilationTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
	public class LinqTransformerCompilationTests : NoDisposalNeeded
	{
		private const string query =
			@"
	from doc in docs
	where doc.Type == ""page""
	select new { Key = doc.Title, Value = doc.Content, Size = doc.Size };
";

		[Fact]
		public void Will_compile_query_successfully()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query },  ".");
			dynamicQueryCompiler.GenerateInstance();
			var compiled = dynamicQueryCompiler.GeneratedType;
			Assert.NotNull(compiled);
		}

		[Fact]
		public void Will_keep_cast_in_query()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("caster", new IndexDefinition { Map = "from x in docs select new { Id = (int)x.Id }" },  ".");
			dynamicQueryCompiler.GenerateInstance();

			Assert.Contains("(int)x.Id", dynamicQueryCompiler.CompiledQueryText);
		}

		[Fact]
		public void Can_create_new_instance_from_query()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query },  ".");
			dynamicQueryCompiler.GenerateInstance();
			var compiled = dynamicQueryCompiler.GeneratedType;
			Activator.CreateInstance(compiled);
		}

		[Fact]
		public void Can_execute_query()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query },  ".");
			var generator = dynamicQueryCompiler.GenerateInstance();
			var results = generator.MapDefinitions[0](new[]
			{
				GetDocumentFromString(
					@"
				{
					'@metadata': {'@id': 1},
					'Type': 'page',
					'Title': 'doc1',
					'Content': 'Foobar',
					'Size': 31
				}")
				,
				GetDocumentFromString(
					@"
				{
					'@metadata': {'@id': 2},
					'Type': 'not a page',
				}")
				,
				GetDocumentFromString(
					@"
				{
					'@metadata': {'@id': 3},
					'Type': 'page',
					'Title': 'doc2',
					'Content': 'Foobar',
					'Size': 31
				}")
				,
			}).Cast<object>().ToArray();

			var expected = new[]
			{
				"{ Key = doc1, Value = Foobar, Size = 31, __document_id = 1 }",
				"{ Key = doc2, Value = Foobar, Size = 31, __document_id = 3 }"
			};

			for (var i = 0; i < results.Length; i++)
			{
				Assert.Equal(expected[i], results[i].ToString());
			}
		}

		[Fact]
		public void Can_compile_map_using_linq_methods()
		{
			var viewGenerator = new DynamicViewCompiler("test", new IndexDefinition
			{
				Map = @"docs.Users
	.Select(user => new {Location = user.Location, Count = 1})
	.Select(user => new {Location = user.Location})"
			},  ".").GenerateInstance();


			var results = viewGenerator.MapDefinitions[0](new[]
			{
				GetDocumentFromString(
				@"
				{
					'@metadata': {'Raven-Entity-Name': 'Users', '@id': 1},
					'Location': 'Tel Aviv'
				}")
			}).Cast<object>().ToArray();

			var expected = new[]
			{
				"{ Location = Tel Aviv, __document_id = 1 }",
			};

			for (var i = 0; i < results.Length; i++)
			{
				Assert.Equal(expected[i], results[i].ToString());
			}
		}

		[Fact]
		public void Can_compile_map_reduce_using_linq_methods()
		{
			var viewGenerator = new DynamicViewCompiler("test", new IndexDefinition
			{
				Map = @"docs.Users
	.Select(user => new {Location = user.Location, Count = 1})",
				Reduce =
					@"results
	.GroupBy(agg => agg.Location)
	.Select(g => new {Location = g.Key, Count = g.Sum(x => x.Count)})"
			},  ".").GenerateInstance();


			var source = viewGenerator.MapDefinitions[0](new[]
			{
				GetDocumentFromString(
					@"
				{
					'@metadata': {'Raven-Entity-Name': 'Users', '@id': 1},
					'Location': 'Tel Aviv'
				}"),
				GetDocumentFromString(
					@"
				{
					'@metadata': {'Raven-Entity-Name': 'Users', '@id': 1},
					'Location': 'Tel Aviv'
				}")
			}).ToArray();
			var results = viewGenerator.ReduceDefinition(source).Cast<object>().ToArray();

			var expected = new[]
			{
				"{ Location = Tel Aviv, Count = 2 }",
			};

			for (var i = 0; i < results.Length; i++)
			{
				Assert.Equal(expected[i], results[i].ToString());
			}
		}


		public static dynamic GetDocumentFromString(string json)
		{
			return JsonToExpando.Convert(RavenJObject.Parse(json));
		}
	}
}
