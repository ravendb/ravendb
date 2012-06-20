//-----------------------------------------------------------------------
// <copyright file="DocumentToJsonAndBackTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Linq;
using Xunit;

namespace Raven.Tests.Bugs
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

			var jObject = RavenJObject.FromObject(page, conventions.CreateSerializer());

			dynamic dynamicObject = new DynamicJsonObject(jObject);
			Assert.NotNull(dynamicObject.CoAuthors as IEnumerable);
			Assert.NotNull(dynamicObject.CoAuthors.Length);
			Assert.Equal(2, dynamicObject.CoAuthors.Length);
		}

		[Fact]
		public void ListOnDynamicJsonObjectFromJsonIsAnArray()
		{
			var conventions = new DocumentConvention();
			var jObject = RavenJObject.FromObject(page, conventions.CreateSerializer());

			dynamic dynamicObject = new DynamicJsonObject(jObject);
			Assert.NotNull(dynamicObject.CoAuthors as IEnumerable);

			Assert.NotNull(dynamicObject.CoAuthors.Length);
			Assert.Equal(2, dynamicObject.CoAuthors.Length);
		}

		[Fact]
		public void LinqQueryWithStaticCallOnEnumerableIsTranslatedToExtensionMethod()
		{
			var indexDefinition = new IndexDefinitionBuilder<Page>
			{
				Map = pages => from p in pages
							   from coAuthor in p.CoAuthors.DefaultIfEmpty()
							   select new
							   {
								   p.Id,
								   CoAuthorUserID = coAuthor != null ? coAuthor.UserId : -1
							   }
			}.ToIndexDefinition(new DocumentConvention());
			var expectedMapTranslation =
				"docs.Pages\r\n\t.SelectMany(p => p.CoAuthors.DefaultIfEmpty(), (p, coAuthor) => new {Id = p.Id, CoAuthorUserID = coAuthor != null ? coAuthor.UserId : -1})";
			Assert.Equal(expectedMapTranslation, indexDefinition.Map);
		}


		[Fact]
		public void LinqQueryWithStaticCallOnEnumerableIsCanBeCompiledAndRun()
		{
			var indexDefinition = new IndexDefinitionBuilder<Page>
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
													  indexDefinition, ".").
				GenerateInstance();

			var conventions = new DocumentConvention();
			var o = RavenJObject.FromObject(page,conventions.CreateSerializer());
			o["@metadata"] = new RavenJObject {{"Raven-Entity-Name", "Pages"}};
			dynamic dynamicObject = new DynamicJsonObject(o);

			var result = mapInstance.MapDefinitions[0](new[] { dynamicObject }).ToList<object>();
			Assert.Equal("{ Id = 0, CoAuthorUserID = 1, __document_id =  }", result[0].ToString());
			Assert.Equal("{ Id = 0, CoAuthorUserID = 2, __document_id =  }", result[1].ToString());
		}

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

		private class User
		{
			public int UserId;
		}
	}
}