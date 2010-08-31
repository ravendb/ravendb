using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class DocumentToJsonAndBackTest
	{
		private readonly Page page;

		public DocumentToJsonAndBackTest()
		{
			page = new Page();
			page.CoAuthors.Add(new User {UserId = 1});
			page.CoAuthors.Add(new User {UserId = 2});
		}

		[Fact]
		public void ListOnDynamicJsonObjectFromJsonWillFailToBeAJsonList()
		{
			var conventions = new DocumentConvention();

			var jObject = JObject.FromObject(page, conventions.CreateSerializer());

			dynamic dynamicObject = new DynamicJsonObject(jObject);
			Assert.NotNull(dynamicObject.CoAuthors as IEnumerable);
			Assert.NotNull(dynamicObject.CoAuthors.Length);
			Assert.Equal(2, dynamicObject.CoAuthors.Length);
		}

		[Fact]
		public void ListOnDynamicJsonObjectFromJsonIsAnArray()
		{
			var conventions = new DocumentConvention();
			var jObject = JObject.FromObject(page,
											 conventions.CreateSerializer());

			dynamic dynamicObject = new DynamicJsonObject(jObject);
			Assert.NotNull(dynamicObject.CoAuthors as IEnumerable);

			Assert.NotNull(dynamicObject.CoAuthors.Length);
			Assert.Equal(2, dynamicObject.CoAuthors.Length);
		}

		[Fact]
		public void LinqQueryWithStaticCallOnEnumerableIsTranslatedToExtensionMethod()
		{
			var indexDefinition = new IndexDefinition<Page>
			{
				Map = pages => from p in pages
							   from coAuthor in Enumerable.DefaultIfEmpty(p.CoAuthors)
							   select new
							   {
								   p.Id,
								   CoAuthorUserID = coAuthor != null ? coAuthor.UserId : -1
							   }
			}.ToIndexDefinition(new DocumentConvention());
			var expectedMapTranslation =
				@"docs.Pages
	.SelectMany(p => p.CoAuthors.DefaultIfEmpty(), (p, coAuthor) => new {Id = p.Id, CoAuthorUserID = coAuthor != null ? coAuthor.UserId : -1})";
			Assert.Equal(expectedMapTranslation, indexDefinition.Map);
		}


		[Fact]
		public void LinqQueryWithStaticCallOnEnumerableIsCanBeCompiledAndRun()
		{
			var indexDefinition = new IndexDefinition<Page>
			{
				Map = pages => from p in pages
							   from coAuthor in p.CoAuthors.DefaultIfEmpty()
							   select new
							   {
								   p.Id,
								   CoAuthorUserID = coAuthor != null ? coAuthor.UserId : -1
							   }
			}.ToIndexDefinition(new DocumentConvention());

			var mapInstance = new DynamicViewCompiler("testView",
													  indexDefinition, new AbstractDynamicCompilationExtension[] { }).
				GenerateInstance();

			var conventions = new DocumentConvention();
			var o = JObject.FromObject(page,conventions.CreateSerializer());
			o["@metadata"] = new JObject(
				new JProperty("Raven-Entity-Name", "Pages")
				);
			dynamic dynamicObject = new DynamicJsonObject(o);

			var result = mapInstance.MapDefinition(new[] { dynamicObject }).ToList();
			Assert.Equal("{ Id = 0, CoAuthorUserID = 1, __document_id =  }", result[0].ToString());
			Assert.Equal("{ Id = 0, CoAuthorUserID = 2, __document_id =  }", result[1].ToString());
		}

		#region Nested type: Page

		private class Page
		{
			public readonly IList<User> CoAuthors;
#pragma warning disable 0649
			public int Id;
#pragma warning restore 0649
			public Page()
			{
				CoAuthors = new List<User>();
			}
		}

		#endregion

		#region Nested type: User

		private class User
		{
			public int UserId;
		}

		#endregion
	}
}