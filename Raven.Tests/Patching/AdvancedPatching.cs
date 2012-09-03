using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Xunit;
using Raven.Client.Document;
using Raven.Client;
using Raven.Json.Linq;
using Raven.Database.Json;
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
			var doc = RavenJObject.FromObject(test);
			var resultJson = new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest { Script = sampleScript });
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal("Something new", result.Id);
			Assert.Equal(2, result.Comments.Count);
			Assert.Equal("one test", result.Comments[0]);
			Assert.Equal("two", result.Comments[1]);
			Assert.Equal(12144, result.Value);
			Assert.Equal("err!!", resultJson["newValue"]);
		}

		[Fact]
		public void CanRemoveFromCollectionByValue()
		{
			var doc = RavenJObject.FromObject(test);
			var resultJson = new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest
			                                                                              	{
			                                                                              		Script = @"
this.Comments.Remove('two');
"
																							});
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal(new[]{"one","seven"}.ToList(), result.Comments);
		}

		[Fact]
		public void CanRemoveFromCollectionByCondition()
		{
			var doc = RavenJObject.FromObject(test);
			var advancedJsonPatcher = new ScriptedJsonPatcher();
			var resultJson = advancedJsonPatcher.Apply(doc, new ScriptedPatchRequest
			{
				Script = @"
this.Comments.RemoveWhere(function(el) {return el == 'seven';});
"
			});
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

			Assert.Equal(new[] { "one", "two" }.ToList(), result.Comments);
		}

		[Fact]
		public void CanPatchUsingVars()
		{
			var doc = RavenJObject.FromObject(test);
			var resultJson = new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest
			                                                                              	{
			                                                                              		Script = "this.TheName = Name",
																								Values = {{"Name", "ayende"}}
			                                                                              	});
			Assert.Equal("ayende", resultJson.Value<string>("TheName"));
		}

		[Fact]
		public void CannotUseEval()
		{
			var doc = RavenJObject.FromObject(test);
			Assert.Throws<NotSupportedException>(
				() => new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest
				                                                                   	{
				                                                                   		Script = "eval('this.Value = 2')",
				                                                                   	}));
		}

		[Fact]
		public void CanHandleNonsensePatching()
		{
			var doc = RavenJObject.FromObject(test);
			Assert.Throws<InvalidOperationException>(
				() =>
				new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest { Script = "this.Id = 'Somethi" }));
		}

		[Fact]
		public void CanThrowIfValueIsWrong()
		{
			var doc = RavenJObject.FromObject(test);
			var invalidOperationException = Assert.Throws<InvalidOperationException>(
				() => new ScriptedJsonPatcher().Apply(doc, new ScriptedPatchRequest {Script = "raise 'problem'"}));

			Assert.Contains("problem", invalidOperationException.Message);
		}

		[Fact]
		public void CanOutputDebugInformation()
		{
			var doc = RavenJObject.FromObject(test);
			var advancedJsonPatcher = new ScriptedJsonPatcher();
			advancedJsonPatcher.Apply(doc, new ScriptedPatchRequest
			                                                             	{
																				Script = "output(this.Id)"
			                                                             	});
		
			Assert.Equal("someId", advancedJsonPatcher.Debug[0]);
		}

		[Fact]
		public void CannotUseWhile()
		{
			var doc = RavenJObject.FromObject(test);
			var advancedJsonPatcher = new ScriptedJsonPatcher();
			Assert.Throws<NotSupportedException>(() => advancedJsonPatcher.Apply(doc, new ScriptedPatchRequest
			                                                                         	{
			                                                                         		Script = "while(true) {}"
			                                                                         	}));
		}

		[Fact]
		public void DefiningFunctionForbidden()
		{
			var doc = RavenJObject.FromObject(test);
			var advancedJsonPatcher = new ScriptedJsonPatcher();
			Assert.Throws<NotSupportedException>(() => advancedJsonPatcher.Apply(doc, new ScriptedPatchRequest
			{
				Script = "function a() { a(); }"
			}));
		}

		[Fact]
		public void LongStringRejected()
		{
			var doc = RavenJObject.FromObject(test);
			var advancedJsonPatcher = new ScriptedJsonPatcher();
			Assert.Throws<NotSupportedException>(() => advancedJsonPatcher.Apply(doc, new ScriptedPatchRequest
			{
				Script = new string('a', 8193)
			}));
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
		public void CanPerformAdvancedPatching_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				ExecuteTest(store);
			}
		}


		[Fact]
		public void CanPerformAdvancedWithSetBasedUpdates_Remotely()
		{
			using (GetNewServer()) 
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

		[Fact]
		public void CanUpdateBasedOnAnotherDocumentProperty()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new CustomType { Value = 2});
					s.Store(new CustomType {  Value = 1 });
					s.SaveChanges();
				}

				store.DatabaseCommands.Patch("CustomTypes/1", new ScriptedPatchRequest
				                                              	{
				                                              		Script = @"
var another = LoadDocument(anotherId);
this.Value = another.Value;
",
				                                              		Values = {{"anotherId", "CustomTypes/2"}}
				                                              	});

				var resultDoc = store.DatabaseCommands.Get("CustomTypes/1");
				var resultJson = resultDoc.DataAsJson;
				var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

				Assert.Equal(1, result.Value);
			}
		}

		private void ExecuteTest(IDocumentStore store)
		{
			using (var s = store.OpenSession())
			{
				s.Store(test);
				s.SaveChanges();
			}

			store.DatabaseCommands.Patch(test.Id, new ScriptedPatchRequest { Script = sampleScript });

			var resultDoc = store.DatabaseCommands.Get(test.Id);
			var resultJson = resultDoc.DataAsJson;
			var result = JsonConvert.DeserializeObject<CustomType>(resultJson.ToString());

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
											new ScriptedPatchRequest { Script = sampleScript });

			var item1ResultJson = store.DatabaseCommands.Get(item1.Id).DataAsJson;
			var item1Result = JsonConvert.DeserializeObject<CustomType>(item1ResultJson.ToString());			
			Assert.Equal(2, item1Result.Comments.Count);
			Assert.Equal("one test", item1Result.Comments[0]);
			Assert.Equal("two", item1Result.Comments[1]);
			Assert.Equal(12144, item1Result.Value);
			Assert.Equal("err!!", item1ResultJson["newValue"]);

			var item2ResultJson = store.DatabaseCommands.Get(item2.Id).DataAsJson;
			var item2Result = JsonConvert.DeserializeObject<CustomType>(item2ResultJson.ToString());			
			Assert.Equal(9999, item2Result.Value);
			Assert.Equal(3, item2Result.Comments.Count);
			Assert.Equal("one", item2Result.Comments[0]);
			Assert.Equal("two", item2Result.Comments[1]);
			Assert.Equal("seven", item2Result.Comments[2]);
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
