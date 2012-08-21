//-----------------------------------------------------------------------
// <copyright file="ProjectingFromIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Database.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ProjectingFromIndexes : RavenTest
	{
		[Fact]
		public void CanProjectFromIndex()
		{
			using (var documentStore = NewDocumentStore())
			using (var httpServer = new HttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
			{
				httpServer.StartListening();
				documentStore.DatabaseCommands.PutIndex("ImagesByTag",
														new IndexDefinitionBuilder<Image, ImageByTagSearchModel>
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
					var imageByTagSearchModels = s.Advanced.LuceneQuery<ImageByTagSearchModel>("ImagesByTag")
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
}
