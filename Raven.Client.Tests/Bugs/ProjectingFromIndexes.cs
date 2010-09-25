using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Database.Server;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class ProjectingFromIndexes : LocalClientTest
    {
        [Fact]
        public void CanProjectFromIndex()
        {
            using (var documentStore = NewDocumentStore())
            using (var httpServer = new HttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
            {
                httpServer.Start();
                documentStore.DatabaseCommands.PutIndex("ImagesByTag",
                                                        new IndexDefinition<Image, ImageByTagSearchModel>
                                                        {
                                                            Map = images => from image in images
                                                                        from tag in image.Tags
                                                                        select new 
                                                                        {
                                                                            TagName = tag,
                                                                            Images = new[] { image.Id }
                                                                        },
                                                            Reduce = results => from result in results
                                                                                group result by result.TagName
                                                                                into g
                                                                                select new
                                                                                {
                                                                                    TagName = g.Key,
                                                                                    Images = g.SelectMany(x => x.Images).Distinct()
                                                                                },
                                                            Stores =

                                                                {
                                                                    {x => x.TagName, FieldStorage.Yes},
                                                                    {x => x.Images, FieldStorage.Yes}
                                                                }
                                                            ,
                                                            Indexes =

                                                                {
                                                                    {x => x.TagName, FieldIndexing.NotAnalyzed},
                                                                    {x => x.Images, FieldIndexing.No}
                                                                }
                                                        },true);

                using(var s = documentStore.OpenSession())
                {
                    s.Store(new Image
                    {
                        Id = "images/123",
                        Tags = new[]
                        {
                            "sport", "footbool"
                        }
                    });

                    s.Store(new Image
                    {
                        Id = "images/234",
                        Tags = new[]
                        {
                            "footbool", "live"
                        }
                    });

                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var imageByTagSearchModels = s.LuceneQuery<ImageByTagSearchModel>("ImagesByTag")
                        .OrderBy("TagName")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal("footbool", imageByTagSearchModels[0].TagName);
                    Assert.Equal(2, imageByTagSearchModels[0].Images.Length);

                    Assert.Equal("live", imageByTagSearchModels[1].TagName);
                    Assert.Equal(1, imageByTagSearchModels[1].Images.Length);

                    Assert.Equal("sport", imageByTagSearchModels[2].TagName);
                    Assert.Equal(1, imageByTagSearchModels[2].Images.Length);
                }
            }
        }
    }

    public class ImageByTagSearchModel
    {
        public string TagName { get; set; }
        public string[] Images { get; set; }
    }

    public class Image
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
    }
}