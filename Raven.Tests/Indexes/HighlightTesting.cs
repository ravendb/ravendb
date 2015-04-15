using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class HighlightTesting : RavenTest
	{
		[Fact]
		public void HighlightText()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog and his owner"
			};

			var searchFor = "about";
			using(var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                WaitForIndexing(store);
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
				                     .WaitForNonStaleResults()
				                     .Highlight("Name", 128, 1, out nameHighlighting)
									 .Search("Name", searchFor)
				                     .ToArray();
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("This is a sample <b style=\"background:yellow\">about</b> a dog and his owner",
				             nameHighlighting.GetFragments("searchitems/1").First());
			}
		}

		[Fact]
		public void HighlightText_CutAfterDot()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a. sample about a dog and his owner"
			};

			var searchFor = "about";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
									 .WaitForNonStaleResults()
									 .Highlight("Name", 128, 1, out nameHighlighting)
									 .Search("Name", searchFor)
									 .ToArray();
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("sample <b style=\"background:yellow\">about</b> a dog and his owner",
							 nameHighlighting.GetFragments("searchitems/1").First());
			}
		}

		[Fact]
		public void HighlightText_LineReturnedShorterThenOriginal()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog and his owner"
			};

			var searchFor = "about";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
									 .WaitForNonStaleResults()
									 .Highlight("Name", 20, 1, out nameHighlighting)
									 .Search("Name", searchFor)
									 .ToArray();
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("sample <b style=\"background:yellow\">about</b> a dog",
							 nameHighlighting.GetFragments("searchitems/1").First());
			}
		}

		[Fact] public void HighlightText_CantFindWork()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog and his owner"
			};

			var searchFor = "cat";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
									 .WaitForNonStaleResults()
									 .Highlight("Name", 20, 1, out nameHighlighting)
									 .Search("Name", searchFor)
									 .ToArray();
				Assert.Empty(nameHighlighting.GetFragments("searchitems/1"));
			}
		}

		[Fact]
		public void HighlightText_FindAllReturences()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = @"Once there lived a dog. He was very greedy. There were many times that he had to pay for his greed. Each time the dog promised himself, “I have learnt my lesson. Now I will never be greedy again.” But he soon forgot his promises and was as greedy as ever.
One afternoon, the dog was terribly hungry. He decided to go look for something to eat. Just outside his house, there was a bridge. “I will go and look for food on the other side of the bridge. The food there is definitely better,” he thought to himself.
He walked across the wooden bridge and started sniffing around for food. Suddenly, he spotted a bone lying at a distance. “Ah, I am in luck. This looks a delicious bone,” he said.
Without wasting any time, the hungry dog picked up the bone and was just about to eat it, when he thought, “Somebody might see here with this bone and then I will have to share it with them. So, I had better go home and eat it.” Holding the bone in his mouth, he ran towards his house.
While crossing the wooden bridge, the dog looked down into the river. There he saw his own reflection. The foolish dog mistook it for another dog. “There is another dog in the water with bone in its mouth,” he thought. Greedy, as he was, he thought, “How nice it would be to snatch that piece of bone as well. Then, I will have two bones.”
So, the greedy dog looked at his reflection and growled. The reflection growled back, too. This made the dog angry. He looked down at his reflection and barked, “Woof! Woof!” As he opened his mouth, the bone in his mouth fell into the river. It was only when the water splashed that the greedy dog realized that what he had seen was nothing but his own reflections and not another dog. But it was too late. He had lost the piece of bone because of his greed. Now he had to go hungry."
			};

			var searchFor = "dog";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
									 .WaitForNonStaleResults()
									 .Highlight("Name", 128, 20, out nameHighlighting)
									 .Search("Name", searchFor)
									 .ToArray();
				var fragments = nameHighlighting.GetFragments("searchitems/1");
				Assert.NotEmpty(fragments);
				int counter = 0;

				foreach (var fragment in fragments)
				{
					counter += Regex.Matches(fragment, "<b style=\"background").Count;
				}
				Assert.Equal(12, counter);
			}
		}

		[Fact]
		public void HighlightText_FindAllReturencesWithSeveralWords()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name =
					@"Once there lived a dog. He was very greedy. There were many times that he had to pay for his greed. Each time the dog promised himself, “I have learnt my lesson. Now I will never be greedy again.” But he soon forgot his promises and was as greedy as ever.
One afternoon, the dog was terribly hungry. He decided to go look for something to eat. Just outside his house, there was a bridge. “I will go and look for food on the other side of the bridge. The food there is definitely better,” he thought to himself.
He walked across the wooden bridge and started sniffing around for food. Suddenly, he spotted a bone lying at a distance. “Ah, I am in luck. This looks a delicious bone,” he said.
Without wasting any time, the hungry dog picked up the bone and was just about to eat it, when he thought, “Somebody might see here with this bone and then I will have to share it with them. So, I had better go home and eat it.” Holding the bone in his mouth, he ran towards his house.
While crossing the wooden bridge, the dog looked down into the river. There he saw his own reflection. The foolish dog mistook it for another dog. “There is another dog in the water with bone in its mouth,” he thought. Greedy, as he was, he thought, “How nice it would be to snatch that piece of bone as well. Then, I will have two bones.”
So, the greedy dog looked at his reflection and growled. The reflection growled back, too. This made the dog angry. He looked down at his reflection and barked, “Woof! Woof!” As he opened his mouth, the bone in his mouth fell into the river. It was only when the water splashed that the greedy dog realized that what he had seen was nothing but his own reflections and not another dog. But it was too late. He had lost the piece of bone because of his greed. Now he had to go hungry."
			};

			var searchFor = "dog look";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
                var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
				                     .WaitForNonStaleResults()
				                     .Highlight("Name", 128, 20, out nameHighlighting)
				                     .Search("Name", searchFor)
				                     .ToArray();
				var fragments = nameHighlighting.GetFragments("searchitems/1");
				Assert.NotEmpty(fragments);
				int counter = 0;

				foreach (var fragment in fragments)
				{
					counter += Regex.Matches(fragment, "<b style=\"background").Count;
				}
				Assert.Equal(14, counter);
			}
		}

		// this one passes now
		[Fact]
		public void HighlightText_WithProjection()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog and his owner"
			};

			var searchFor = "about";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
				var results = session.Advanced.DocumentQuery<SearchItem>("ContentSearchIndex")
									 .WaitForNonStaleResults()
									 .Highlight("Name", 128, 1, out nameHighlighting)
									 .Search("Name", searchFor)
									 .SelectFields<SearchItemProjection>()
									 .ToArray();
				Assert.Equal(1, results.Length);
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("This is a sample <b style=\"background:yellow\">about</b> a dog and his owner",
							 nameHighlighting.GetFragments("searchitems/1").First());
			}
		}

		// this one passes now as well
		[Fact]
		public void HighlightText_WithProjectionQuery()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog and his owner"
			};

			var searchFor = "about";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting = null;
				var results = session.Query<SearchItem>("ContentSearchIndex")
									 .ProjectFromIndexFieldsInto<SearchItemProjection>()
									 .Customize(x => x.WaitForNonStaleResults()
													  .Highlight("Name", 128, 1, out nameHighlighting))
									 .Search(x => x.Name, searchFor)
									 .ToArray();
				Assert.Equal(1, results.Length);
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("This is a sample <b style=\"background:yellow\">about</b> a dog and his owner",
							 nameHighlighting.GetFragments("searchitems/1").First());
			}
		}

		// this one fails
		[Fact]
		public void HighlightText_WithMapReduce()
		{
			var item1 = new SearchItemWithType
			{
				Id = "searchitems/1",
				Type = "Fact",
				Text = "Once there lived a dog."
			};

			var item2 = new SearchItemWithType
			{
				Id = "searchitems/2",
				Type = "Opinion",
				Text = "The dog was very greedy."
			};

			var item3 = new SearchItemWithType
			{
				Id = "searchitems/3",
				Type = "Opinion",
				Text = "He had lost the piece of dog bone because of his greed."
			};

			var searchFor = "dog";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item1);
				session.Store(item2);
				session.Store(item3);
				store.DatabaseCommands.PutIndex(new ContentSearchMapReduceIndex().IndexName, new ContentSearchMapReduceIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
				var results = session.Advanced.DocumentQuery<SearchItemWithTypeResult>("ContentSearchMapReduceIndex")
									 .WaitForNonStaleResults()
									 .Highlight("AllText", "Type", 128, 1, out nameHighlighting)
									 .Search("AllText", searchFor)
									 .ToArray();

				Assert.Equal(2, results.Length);

				var opinionFragments = nameHighlighting.GetFragments("Opinion");
				Assert.Equal(1, opinionFragments.Length);
				Assert.Equal("The <b style=\"background:yellow\">dog</b> was very greedy. He had lost the piece of <b style=\"background:yellow\">dog</b> bone because of his greed.", opinionFragments[0]);

				var factFragments = nameHighlighting.GetFragments("Fact");
				Assert.Equal(1, factFragments.Length);
				Assert.Equal("Once there lived a <b style=\"background:yellow\">dog</b>.", factFragments[0]);
			}
		}

		// this one passes
		[Fact]
		public void HighlightText_WithMapReduceWithFragments()
		{
			var item1 = new SearchItemWithType
			{
				Id = "searchitems/1",
				Type = "Fact",
				Text = "Once there lived a dog."
			};

			var item2 = new SearchItemWithType
			{
				Id = "searchitems/2",
				Type = "Opinion",
				Text = "The dog was very greedy."
			};

			var item3 = new SearchItemWithType
			{
				Id = "searchitems/3",
				Type = "Opinion",
				Text = "He had lost the piece of dog bone because of his greed."
			};

			var searchFor = "dog";
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(item1);
				session.Store(item2);
				session.Store(item3);
				store.DatabaseCommands.PutIndex(new ContentSearchMapReduceIndex().IndexName, new ContentSearchMapReduceIndex().CreateIndexDefinition());
				session.SaveChanges();

				var results = session.Advanced.DocumentQuery<SearchItemWithTypeResultWithFragments>("ContentSearchMapReduceIndex")
									 .WaitForNonStaleResults()
									 .Highlight("AllText", 128, 1, "Fragments")
									 .Search("AllText", searchFor)
									 .ToArray();

				Assert.Equal(2, results.Length);

				var opinionResult = results.First(r => r.Type == "Opinion");
				var opinionFragments = opinionResult.Fragments;
				Assert.Equal(1, opinionFragments.Length);
				Assert.Equal("The <b style=\"background:yellow\">dog</b> was very greedy. He had lost the piece of <b style=\"background:yellow\">dog</b> bone because of his greed.", opinionFragments[0]);

				var factResult = results.First(r => r.Type == "Fact");
				var factFragments = factResult.Fragments;
				Assert.Equal(1, factFragments.Length);
				Assert.Equal("Once there lived a <b style=\"background:yellow\">dog</b>.", factFragments[0]);
			}
		}
	}

	public class ContentSearchIndex : AbstractIndexCreationTask<SearchItem>
	{
		public ContentSearchIndex()
		{
			Map = (docs => from doc in docs
			                           select new {doc.Name});

			Index(x => x.Name, FieldIndexing.Analyzed);
			Store(x => x.Name, FieldStorage.Yes);
			TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
		}
	}

	public class SearchItem
	{
		public string Name { get; set; }
		public string Id { get; set; }
	}

	public class SearchItemProjection
	{
		public string Name { get; set; }
		public string Id { get; set; }
	}

	public class ContentSearchMapReduceIndex : AbstractIndexCreationTask<SearchItemWithType, SearchItemWithTypeResult>
	{
		public ContentSearchMapReduceIndex()
		{
			Map = (docs => from doc in docs
						   select new { doc.Type, AllText = doc.Text, Count = 1 });

			Reduce = (results => from result in results
									group result by result.Type
									into g
									select new { Type = g.Key, AllText = string.Join(" ", g.Select(r => r.AllText)), Count = g.Sum(r => r.Count) });

			Index(x => x.AllText, FieldIndexing.Analyzed);
			Store(x => x.AllText, FieldStorage.Yes);
			Store(x => x.Type, FieldStorage.Yes);
			TermVector(x => x.AllText, FieldTermVector.WithPositionsAndOffsets);
		}
	}

	public class SearchItemWithType
	{
		public string Type { get; set; }
		public string Text { get; set; }
		public string Id { get; set; }
	}

	public class SearchItemWithTypeResult
	{
		public string Type { get; set; }
		public string AllText { get; set; }
		public int Count { get; set; }
	}

	public class SearchItemWithTypeResultWithFragments : SearchItemWithTypeResult
	{
		public string[] Fragments { get; set; }
	}
}
