// -----------------------------------------------------------------------
//  <copyright file="ScriptedIndexResultsTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
	public class ScriptedIndexResultsTest : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
		}

		[Fact]
		public void CanUpdateValueOnDocument()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Raven.Abstractions.Data.ScriptedIndexResults
					{
						Id = Raven.Abstractions.Data.ScriptedIndexResults.IdPrefix + new Animals_Stats().IndexName,
						IndexScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId) || {};
type.Count = this.Count;
PutDocument(docId, type);",
						DeleteScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId);
if(type == null)
	return;
type.Count = 0;
PutDocument(docId, type);
"
					});
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					s.Store(new Animal
					{
						Name = "Arava",
						Type = "Dog"
					});
					s.Store(new Animal
					{
						Name = "Oscar",
						Type = "Dog"
					});

					s.Store(new AnimalType
					{
						Id = "AnimalTypes/Dog",
						Description = "Man's Best Friend"
					});

					s.SaveChanges();
				}

				new Animals_Stats().Execute(store);

				WaitForIndexing(store);

				using (var s = store.OpenSession())
				{
					var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
					Assert.Equal(2, animalType.Count);
				}
			}
		}

		[Fact]
		public void CanUpdateValueOnDocumentWhenItemIsRemoved()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Raven.Abstractions.Data.ScriptedIndexResults
					{
						Id = Raven.Abstractions.Data.ScriptedIndexResults.IdPrefix + new Animals_Stats().IndexName,
						IndexScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId) || {};
type.Count = this.Count;
PutDocument(docId, type);",
						DeleteScript = @"
var docId = 'AnimalTypes/'+ key;
var type = LoadDocument(docId);
if(type == null)
	return;
type.Count = 0;
PutDocument(docId, type);
"
					});
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					s.Store(new Animal
					{
						Name = "Arava",
						Type = "Dog"
					});
					s.Store(new Animal
					{
						Name = "Oscar",
						Type = "Dog"
					});

					s.Store(new AnimalType
					{
						Id = "AnimalTypes/Dog",
						Description = "Man's Best Friend"
					});

					s.SaveChanges();
				}

				new Animals_Stats().Execute(store);

				WaitForIndexing(store);

				store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:Animals"
				}).WaitForCompletion();
				
				WaitForIndexing(store);

				using (var s = store.OpenSession())
				{
					var animalType = s.Load<AnimalType>("AnimalTypes/Dog");
					Assert.Equal(0, animalType.Count);
				}
			}
		}
	}
}