using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MoreLikeThisTrack : RavenTest
	{
		public class Track
		{
			public string Title { get; set; }
			public string Artist { get; set; }
			public string Genre { get; set; }
			public int? Year { get; set; }
		}

		public class TracksIndex : AbstractIndexCreationTask<Track>
		{
			public TracksIndex()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  doc.Title,
								  doc.Artist,
								  doc.Genre,
								  doc.Year,
								  FreeText = new object[] { doc.Title, doc.Artist, doc.Genre, doc.Year }
							  };

				Sort(x => x.Year, SortOptions.Short);

				Index("FreeText", FieldIndexing.Analyzed);

				Store(x => x.Genre, FieldStorage.Yes);
				Store(x => x.Artist, FieldStorage.Yes);
				Store("FreeText", FieldStorage.Yes);
			}
		}

		[Fact]
		public void Should_find_similar_tracks()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new TracksIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Track
					{
						Artist = "Bryan Adams",
						Title = "Star",
						Genre = "Rock",
						Year = 2005,
					}, "tracks/1");

					session.Store(new Track
					{
						Artist = "Bryan Adams",
						Title = "Please Forgive Me",
						Genre = "Rock",
						Year = 2002,
					}, "tracks/2");

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var mlt = session.Advanced.MoreLikeThis<Track, TracksIndex>(new MoreLikeThisQueryParameters
					{
						DocumentId = "tracks/1",
						Fields = new[] { "Genre", "Artist" },
						IndexName = "TracksIndex",
						MinimumTermFrequency = 0,
						MinimumDocumentFrequency = 0,
					});
					Assert.NotEmpty(mlt);

					mlt = session.Advanced.MoreLikeThis<Track, TracksIndex>(new MoreLikeThisQueryParameters
					{
						DocumentId = "tracks/1",
						Fields = new[] { "FreeText" },
						IndexName = "TracksIndex",
						MinimumTermFrequency = 0,
						MinimumDocumentFrequency = 0,

					});
					Assert.NotEmpty(mlt);

					mlt = session.Advanced.MoreLikeThis<Track, TracksIndex>(new MoreLikeThisQueryParameters
					{
						MinimumTermFrequency = 0,
						MinimumDocumentFrequency = 0,
						DocumentId = "tracks/1"
					});
					Assert.NotEmpty(mlt);
				}
			}
		}
	}
}
