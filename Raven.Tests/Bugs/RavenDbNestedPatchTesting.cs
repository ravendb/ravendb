using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class RavenDbNestedPatchTesting : RavenTest
	{
		[Fact]
		public void RavenTestArrayPatchingRemove()
		{
			using(GetNewServer())
			using (var docStore = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				const string trackId = "tracks/3";
				var artist295 = new ArtistInfo {Id = "artists/295", Name = "Bob Dylan"};
				var artist296 = new ArtistInfo {Id = "artists/296", Name = "Roy Orbison"};
				var track1 = new MultipleArtistTrack
				{Id = trackId, Name = "Handle With Care", Artists = new[] {artist295, artist296}};

				using (IDocumentSession session = docStore.OpenSession())
				{
					session.Store(track1);
					session.SaveChanges();
				}

				int removedPosition = 1;

				BatchResult[] result = docStore.DatabaseCommands.Batch(new[]
				{
					new PatchCommandData
					{
						Key = trackId,
						Patches = new[]
						{
							new PatchRequest
							{
								Type = PatchCommandType.Remove,
								Name = "Artists",
								Position = removedPosition
								//Value = RavenJToken.FromObject(artist296)
							}
						}
					},
				});

				using (IDocumentSession session = docStore.OpenSession())
				{
					track1 = session.Load<MultipleArtistTrack>(trackId);
				}

				Assert.Equal(1, track1.Artists.Length);
			}
		}

		#region Nested type: ArtistInfo

		public class ArtistInfo
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		#endregion

		#region Nested type: MultipleArtistTrack

		public class MultipleArtistTrack
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public ArtistInfo[] Artists { get; set; }
		}

		#endregion
	}
}