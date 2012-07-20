using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Raven.Client.Document;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Database.Json;
using Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Exceptions;

namespace Raven.Tests.Patching
{
	public class AdvancedPatching : RavenTest
	{
		CustomType test = new CustomType
			{
				Id = "someId",
				Owner = "bob",
				Value = 12143,
				Comments = new List<string>(new[] { "one", "two", "seven" })
			};

		//splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
		string sampleScript = @"
	this.Id = 'Something new'; 
	this.Value++; 
	this.Comments.splice(2, 1);
	this.newValue = ""err!!"";
	this.Comments.Map(function(comment) {   
		return (comment == ""one"") ? comment + "" test"" : comment;
	});";

		[Fact]
		public void CanApplyBasicScriptAsPatch()
		{
			var resultJson = new AdvancedJsonPatcher(RavenJObject.FromObject(test)).Apply(new AdvancedPatchRequest { Script = sampleScript });
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal("Something new", result.Id);
			Assert.Equal(2, result.Comments.Count);
			Assert.Equal("one test", result.Comments[0]);
			Assert.Equal("two", result.Comments[1]);
			Assert.Equal(12144, result.Value);
			Assert.Equal("err!!", resultJson["newValue"]);
		}

		[Fact]
		public void CanPerformAdvancedPatching_Remotely()
		{
			using (var server = GetNewServer(port:8079))
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPerformAdvancedPatchingWithConcurrencyException_Remotely()
		{
			using (var server = GetNewServer(port: 8079))
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteConcurrencyExceptionTest(store);
			}
		}

		[Fact]
		public void CanPerformAdvancedPatching_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}

		[Fact]
		public void CanPerformAdvancedPatchingWithConcurrencyException_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteConcurrencyExceptionTest(store);
			}
		}

		[Fact]
		public void CanPerformAdvancedWithSetBasedUpdates_Remotely()
		{
			using (var server = GetNewServer(port: 8079))
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				ExecuteSetBasedTest(store);
			}
		}

		[Fact]
		public void CanPerformAdvancedWithSetBasedUpdates_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteSetBasedTest(store);
			}
		}		

		private void ExecuteTest(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				s.Store(test);
				s.SaveChanges();
			}

			store.DatabaseCommands.Patch(test.Id, new AdvancedPatchRequest { Script = sampleScript });

			// TODO this is wierd, we can change the Id in the Json to something other than the Key
			// so we end up with a do that we can load via "someId" but result.Id = "Something new"
			// we need to make sure the javascript can't change the Id field, or something else!??!
			var resultDoc = store.DatabaseCommands.Get(test.Id);
			var resultJson = resultDoc.DataAsJson;
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			//Assert.Equal("Something new", result.Id);
			Assert.NotEqual("Something new", resultDoc.Metadata["@id"]);
			Assert.Equal(2, result.Comments.Count);
			Assert.Equal("one test", result.Comments[0]);
			Assert.Equal("two", result.Comments[1]);
			Assert.Equal(12144, result.Value);
			Assert.Equal("err!!", resultJson["newValue"]);
		}

		private void ExecuteSetBasedTest(IDocumentStore store)
		{
			var item1 = new CustomType
							{
								Id = "someId/",
								Owner = "bob",
								Value = 12143,
								Comments = new List<string>(new[] {"one", "two", "seven"})
							};
			var item2 = new CustomType
							{
								Id = "someId/",
								Owner = "NOT bob",
								Value = 9999,
								Comments = new List<string>(new[] {"one", "two", "seven"})
							};
			
			using (var s = store.OpenSession())
			{
				s.Store(item1);
				s.Store(item2);
				s.SaveChanges();
			}

			store.DatabaseCommands.PutIndex("TestIndex", 
					new IndexDefinition
					{
						Map = @"from doc in docs 
									select new { doc.Owner }"
					});

			store.OpenSession().Advanced.LuceneQuery<CustomType>("TestIndex")
					.WaitForNonStaleResults().ToList();

			store.DatabaseCommands.UpdateByIndex("TestIndex",
											new IndexQuery { Query = "Owner:Bob" },
											new AdvancedPatchRequest { Script = sampleScript });

			var item1ResultJson = store.DatabaseCommands.Get(item1.Id).DataAsJson;
			var item1Result = JsonConvert.DeserializeObject<CustomType>(item1ResultJson.ToString());			
			Assert.Equal(2, item1Result.Comments.Count);
			Assert.Equal("one test", item1Result.Comments[0]);
			Assert.Equal("two", item1Result.Comments[1]);
			Assert.Equal(12144, item1Result.Value);
			Assert.Equal("err!!", item1ResultJson["newValue"]);

			var item2ResultJson = store.DatabaseCommands.Get(item2.Id).DataAsJson;
			var item2Result = JsonConvert.DeserializeObject<CustomType>(item2ResultJson.ToString());			
			//Assert.Equal(item2, item2Result);
			var doc = store.DatabaseCommands.Get(item2.Id);
			//TODO work out why the @id metadata item is missing here (when run in client server mode)?
			var metadata = store.DatabaseCommands.Get(item2.Id).Metadata["@id"];
			//Assert.True(metadata.ToString().StartsWith("someId"));
			Assert.Equal(9999, item2Result.Value);
			Assert.Equal(3, item2Result.Comments.Count);
			Assert.Equal("one", item2Result.Comments[0]);
			Assert.Equal("two", item2Result.Comments[1]);
			Assert.Equal("seven", item2Result.Comments[2]);
		}

		private void ExecuteConcurrencyExceptionTest(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				s.Store(test);
				s.SaveChanges();
			}

			//Delibrately set the prevVal Json to something else, so it throws
			var testAsJson = RavenJObject.FromObject(test);
			testAsJson["Value"] = 999;
			Assert.Throws<ConcurrencyException>(() =>
			store.DatabaseCommands.Patch(test.Id, new AdvancedPatchRequest { Script = sampleScript, PrevVal = testAsJson }));
		}

		class CustomType
		{
			public string Id { get; set; }
			public string Owner { get; set; }
			public int Value { get; set; }
			public List<string> Comments { get; set; }
		}
	}
}
