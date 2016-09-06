using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace SlowTests.MailingList
{
    public class Rob : RavenTestBase
    {

        [Fact]
        public void CanUseIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Article_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    var visibility = "playerId";
                    var all = "allId";

                    session.Query<CampaignIndexEntry, Article_Index>()
                        .Where(x => x.CampaignId == "someCampaignId")
                        .Where(x => x.VisibleTo.Equals(visibility) || x.VisibleTo.Equals(all))
                        .ToArray();

                }
            }

        }

        private class Article
        {
            public string Id { get; set; }
            public string CampaignId { get; set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> VisibleTo { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> Tags { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IEnumerable<string> TagsAsSlugs
            {
                get { return Tags.Select(x => x); }
            }

            public Article()
            {
                VisibleTo = new List<string>();
                Tags = new List<string>();
            }
        }

        private class Post
        {
            public string Id { get; set; }
            public string CampaignId { get; set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> VisibleTo { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> Tags { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IEnumerable<string> TagsAsSlugs
            {
                get { return Tags.Select(x => x); }
            }

            public Post()
            {
                Tags = new List<string>();
                VisibleTo = new List<string>();
            }
        }

        private class Scene
        {
            public string Id { get; set; }
            public string CampaignId { get; set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> VisibleTo { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IList<string> Tags { get; private set; }

            [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
            public IEnumerable<string> TagsAsSlugs
            {
                get { return Tags.Select(x => x); }
            }

            public Scene()
            {
                Tags = new List<string>();
                VisibleTo = new List<string>();
            }
        }

        private class CampaignIndexEntry
        {
            public string CampaignId { get; set; }
            public string TagSlug { get; set; }
            public int Count { get; set; }
            public IEnumerable<string> VisibleTo { get; set; }
        }

        private class Article_Index : AbstractMultiMapIndexCreationTask<CampaignIndexEntry>
        {
            public Article_Index()
            {
                AddMap<Article>(
                    articles => from article in articles
                                from tag in article.TagsAsSlugs
                                select new
                                {
                                    article.CampaignId,
                                    TagSlug = tag,
                                    Count = 1,
                                    article.VisibleTo
                                });

                AddMap<Post>(
                    posts => from post in posts
                             from tag in post.TagsAsSlugs
                             select new
                             {
                                 post.CampaignId,
                                 TagSlug = tag,
                                 Count = 1,
                                 post.VisibleTo
                             });

                AddMap<Scene>(
                    scenes => from scene in scenes
                              from tag in scene.TagsAsSlugs
                              select new
                              {
                                  scene.CampaignId,
                                  TagSlug = tag,
                                  Count = 1,
                                  scene.VisibleTo
                              });

                Reduce = results => from result in results
                                    group result by new { result.CampaignId, result.TagSlug }
                                        into g
                                    select new
                                    {
                                        g.Key.CampaignId,
                                        g.Key.TagSlug,
                                        Count = g.Sum(x => x.Count),
                                        VisibleTo = g.SelectMany(x => x.VisibleTo).Distinct()
                                    };
            }
        }
    }
}
