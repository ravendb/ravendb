using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CreateIndexesRemotely :RemoteClientTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.Get;
		}

		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void CanDoSo_DirectUrl()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				var container = new CompositionContainer(new TypeCatalog(typeof(Posts_ByMonthPublished_Count), typeof(Tags_Count)));
				IndexCreation.CreateIndexes(container, store);
			}
		}

		[Fact]
		public void CanDoSo_ConnectionString()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { ConnectionStringName = "Server" }.Initialize())
			{
				var container = new CompositionContainer(new TypeCatalog(typeof(Posts_ByMonthPublished_Count), typeof(Tags_Count)));
				IndexCreation.CreateIndexes(container, store);
			}
		}


		public class Posts_ByMonthPublished_Count : AbstractIndexCreationTask<Post, PostCountByMonth>
		{
			public Posts_ByMonthPublished_Count()
			{
				Map = posts => from post in posts
							   select new { post.PublishAt.Year, post.PublishAt.Month, Count = 1 };
				Reduce = results => from result in results
									group result by new { result.Year, result.Month }
										into g
										select new { g.Key.Year, g.Key.Month, Count = g.Sum(x => x.Count) };
			}
		}

		public class Tags_Count : AbstractIndexCreationTask<Post, TagCount>
		{
			public Tags_Count()
			{
				Map = posts => from post in posts
							   from tag in post.Tags
							   select new { Name = tag, Count = 1, LastSeenAt = post.PublishAt };
				Reduce = results => from tagCount in results
									group tagCount by tagCount.Name
										into g
										select new { Name = g.Key, Count = g.Sum(x => x.Count), LastSeenAt = g.Max(x => (DateTimeOffset)x.LastSeenAt) };
			}
		}

		public class PostCountByMonth
		{
			public int Year { get; set; }
			public int Month { get; set; }
			public int Count { get; set; }
		}

		public class TagCount
		{
			public string Name { get; set; }
			public int Count { get; set; }
			public DateTimeOffset LastSeenAt { get; set; }
		}

		public class Post
		{
			public string Id { get; set; }

			public string Title { get; set; }
			public string LegacySlug { get; set; }

			public string Body { get; set; }
			public ICollection<string> Tags { get; set; }

			public string AuthorId { get; set; }
			public DateTimeOffset CreatedAt { get; set; }
			public DateTimeOffset PublishAt { get; set; }
			public bool SkipAutoReschedule { get; set; }

			public string LastEditedByUserId { get; set; }
			public DateTimeOffset? LastEditedAt { get; set; }

			public bool IsDeleted { get; set; }
			public bool AllowComments { get; set; }

			private Guid _showPostEvenIfPrivate;
			public Guid ShowPostEvenIfPrivate
			{
				get
				{
					if (_showPostEvenIfPrivate == Guid.Empty)
						_showPostEvenIfPrivate = Guid.NewGuid();
					return _showPostEvenIfPrivate;
				}
				set { _showPostEvenIfPrivate = value; }
			}

			public int CommentsCount { get; set; }

			public string CommentsId { get; set; }

			public IEnumerable<string> TagsAsSlugs
			{
				get
				{
					if (Tags == null)
						yield break;
					foreach (var tag in Tags)
					{
						yield return tag;
					}
				}
			}

			public bool IsPublicPost(string key)
			{
				if (PublishAt <= DateTimeOffset.Now && IsDeleted == false)
					return true;

				Guid maybeKey;
				if (key == null || Guid.TryParse(key, out maybeKey) == false)
					return false;

				return maybeKey == ShowPostEvenIfPrivate;
			}
		}
	}
}