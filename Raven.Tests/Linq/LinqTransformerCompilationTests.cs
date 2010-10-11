using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Xunit;

namespace Raven.Tests.Linq
{
	public class LinqTransformerCompilationTests
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
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query }, new AbstractDynamicCompilationExtension[0]);
			dynamicQueryCompiler.GenerateInstance();
			var compiled = dynamicQueryCompiler.GeneratedType;
			Assert.NotNull(compiled);
		}

		[Fact]
		public void Can_create_new_instance_from_query()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query }, new AbstractDynamicCompilationExtension[0]);
			dynamicQueryCompiler.GenerateInstance();
			var compiled = dynamicQueryCompiler.GeneratedType;
			Activator.CreateInstance(compiled);
		}

		[Fact]
		public void Can_execute_query()
		{
			var dynamicQueryCompiler = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query }, new AbstractDynamicCompilationExtension[0]);
			var generator = dynamicQueryCompiler.GenerateInstance();
			var results = generator.MapDefinition(new[]
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
			}, new AbstractDynamicCompilationExtension[0]).GenerateInstance();


			var results = viewGenerator.MapDefinition(new[]
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
		public void Can_compile_map_reudce_using_linq_methods()
		{
			var viewGenerator = new DynamicViewCompiler("test", new IndexDefinition
			{
				Map = @"docs.Users
	.Select(user => new {Location = user.Location, Count = 1})",
				Reduce =
					@"results
	.GroupBy(agg => agg.Location)
	.Select(g => new {Loction = g.Key, Count = g.Sum(x => x.Count}))"
			}, new AbstractDynamicCompilationExtension[0]).GenerateInstance();


			var results = viewGenerator.ReduceDefinition(viewGenerator.MapDefinition(new[]
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
			})).Cast<object>().ToArray();

			var expected = new[]
			{
				"{ Loction = Tel Aviv, Count = 2 }",
			};

			for (var i = 0; i < results.Length; i++)
			{
				Assert.Equal(expected[i], results[i].ToString());
			}
		}


		public static dynamic GetDocumentFromString(string json)
		{
			return JsonToExpando.Convert(JObject.Parse(json));
		}
	}
}
