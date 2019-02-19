using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class CreateIndexesRemotely : RavenTestBase
    {
        [Fact]
        public void CanDoSo_DirectUrl()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndexes(new AbstractIndexCreationTask[] { new PostsByMonthPublishedCount(), new TagsCount() });
            }
        }

        private class PostsByMonthPublishedCount : AbstractIndexCreationTask<Post, PostCountByMonth>
        {
            public PostsByMonthPublishedCount()
            {
                Map = posts => from post in posts
                               select new { post.PublishAt.Year, post.PublishAt.Month, Count = 1 };
                Reduce = results => from result in results
                                    group result by new { result.Year, result.Month }
                                        into g
                                    select new { g.Key.Year, g.Key.Month, Count = g.Sum(x => x.Count) };
            }
        }

        private class TagsCount : AbstractIndexCreationTask<Post, TagCount>
        {
            public TagsCount()
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

        private class PostCountByMonth
        {
            public int Year { get; set; }
            public int Month { get; set; }
            public int Count { get; set; }
        }

        private class TagCount
        {
            public string Name { get; set; }
            public int Count { get; set; }
            public DateTimeOffset LastSeenAt { get; set; }
        }

        private class Post
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
