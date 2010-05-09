using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database;
using Raven.Database.Backup;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Client;

namespace Raven.Tryouts
{
	internal class Program
	{
	

		public static void Main()
		{
			
			try
			{
				using (var documentStore = new DocumentStore { Url = "http://localhost:8080" })
				{
					documentStore.Initialise();

					using (var session = documentStore.OpenSession())
					{
						//Only add the index if there are not posts in the database, i.e. the 1st time this is run!!
						if (session.Query<TagCloud.Post>().Count() == 0)
						{
							Console.WriteLine("First time usage, creating indexes");

							documentStore.DatabaseCommands.PutIndex("TagCloud",
																	new IndexDefinition
																	{
																		Map = @"from post in docs.Posts                                                                    
                                                        from Tag in post.Tags
                                                        select new { Tag, Count = 1 }",

																		Reduce = @"from result in results
                                                        group result by result.Tag into g
                                                        select new { Tag = g.Key, Count = g.Sum(x => (long)x.Count) }"
																	});
						}

						session.Store(new TagCloud.Post { Title = "Title 1", Content = "something", PostedAt = RandomDateTime(), Tags = CreateTags("C#", ".NET") });
						session.Store(new TagCloud.Post { Title = "Title 2", Content = "blah blah", PostedAt = RandomDateTime(), Tags = CreateTags("C#", ".NET") });
						session.SaveChanges();

						var tagCloud = session.Query<TagCloud.TagAndCount>("TagCloud").WaitForNonStaleResults().ToArray();

						Console.WriteLine("\n\nBEFORE: TagCloud has " + tagCloud.Count() + " items :");
						foreach (var tagCount in tagCloud)
							Console.WriteLine("  " + tagCount);

						session.Store(new TagCloud.Post { Title = "Title 1", Content = "something", PostedAt = RandomDateTime(), Tags = CreateTags("C#", ".NET") });
						session.Store(new TagCloud.Post { Title = "Title 2", Content = "blah blah", PostedAt = RandomDateTime(), Tags = CreateTags("C#", ".NET") });
						session.SaveChanges();

						tagCloud = session.Query<TagCloud.TagAndCount>("TagCloud").WaitForNonStaleResults().ToArray();

						Console.WriteLine("\n\nAFTER: TagCloud has " + tagCloud.Count() + " items :");
						foreach (var tagCount in tagCloud)
							Console.WriteLine("  " + tagCount);

					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}

		private static List<string> CreateTags(params string [] arr)
		{
			return new List<string>(arr);
		}

		private static DateTime RandomDateTime()
		{
			return DateTime.Now;
		}
	}

}