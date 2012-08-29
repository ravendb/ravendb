using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Dmitry : RavenTest
	{
		[Fact]
		public void DeepEqualsWorksWithTimeSpan()
		{
			var content = new MusicContent
			{
				Title = String.Format("Song # {0}", 1),
				Album = String.Format("Album # {0}", 1)
			};
			content.Keywords.Add("new");

			var obj = RavenJToken.FromObject(content);
			var newObj = RavenJToken.FromObject(content);

			Assert.True(RavenJToken.DeepEquals(obj, newObj));
		}

		[Fact]
		public void TimeSpanWontTriggerPut()
		{
			using (var store = NewDocumentStore())
			{
				new MusicSearchIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					// Creating a big enough sample to reproduce
					for (int i = 0; i < 100; i++)
					{
						var content = new MusicContent
						              	{
						              		Title = String.Format("Song # {0}", i + 1),
						              		Album = String.Format("Album # {0}", (i%8) + 1)
						              	};

						if (i > 0 && i%10 == 0)
						{
							content.Keywords.Add("new");
						}

						session.Store(content);
					}

					session.SaveChanges();
				}

				WaitForIndexing(store);

				const string Query = "Title:<<new>> Album:<<new>> Keywords:<<new>>";
				using (var session = store.OpenSession())
				{
					var content = session.Advanced
						.LuceneQuery<MusicContent, MusicSearchIndex>()
						.Where(Query)
						.Skip(1)
						.Take(10)
						.ToList();

					Assert.False(session.Advanced.HasChanges);
				}

			}
		}

		public class MusicSearchIndex : AbstractIndexCreationTask<MusicContent, MusicSearchIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public string Id { get; set; }
				public string Title { get; set; }
				public string Album { get; set; }
				public string[] Keywords { get; set; }

			}

			public MusicSearchIndex()
			{
				Map = results => from result in results
								 select new
								 {
									 result.Id,
									 Title = result.Title.Boost(10),
									 result.Album,
									 Keywords = result.Keywords.Boost(5)
								 };

				Index(field => field.Title, FieldIndexing.Analyzed);
				Index(field => field.Album, FieldIndexing.Analyzed);
				Index(field => field.Keywords, FieldIndexing.Default);
			}
		}

		public abstract class Content
		{
			protected Content()
			{
				Keywords = new HashSet<string>();
			}

			public string Id { get; set; }
			public string Title { get; set; }
			public TimeSpan Duration { get; set; } // without this property it works

			public ICollection<string> Keywords { get; protected set; }
		}

		public class MusicContent : Content
		{
			public string Album { get; set; }
		}
	}
}
