using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Stress
{
	public class StressTester : LocalClientTest
	{
		const string FilePath5MB = "Stress\\Data\\data_5MB.txt";
		const string FilePath500KB = "Stress\\Data\\data_500KB.txt";
		const string FilePath100KB = "Stress\\Data\\data_100KB.txt";

		public void munin_stress_testing_ravendb_5mb_in_single_session_in_memory()
		{
			var text = File.ReadAllText(FilePath5MB);

			using (var documentStore = NewDocumentStore("munin", true, text.Length * 102))
			{
				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
					}
					session.SaveChanges();
				}
				Assert.True(true);
			}
		}

		public void munin_stress_testing_ravendb_5mb_in_single_session_in_filesystem()
		{
			var text = File.ReadAllText(FilePath5MB);

			using (var documentStore = NewDocumentStore("munin", false))
			{
				using (var session = documentStore.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
					}
					session.SaveChanges();
				}
				Assert.True(true);
			}
		}

		public void munin_stress_testing_ravendb_simple_object_in_filesystem()
		{
			const string text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla convallis urna eget enim venenatis condimentum. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Suspendisse eget lectus justo, quis condimentum libero. Cras dictum consequat scelerisque. Pellentesque lectus nisi, porttitor id posuere et, dignissim sed orci. Vivamus sollicitudin gravida massa faucibus feugiat";

			using (var documentStore = NewDocumentStore("munin", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject { { "Content", text }, { "Id", RavenJToken.FromObject(Guid.NewGuid()) } });
						}
						session.SaveChanges();
					}
					Console.WriteLine(j);
				}
				Assert.True(true);
			}
		}

		public void munin_stress_testing_ravendb_100kb_in_filesystem()
		{
			var text = File.ReadAllText(FilePath100KB);

			using (var documentStore = NewDocumentStore("munin", false))
			{
				for (int i = 0; i < 100; i++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int j = 0; j < 100; j++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid().ToString())}});
						}
						session.SaveChanges();
					}
				}
				Console.WriteLine(GC.GetTotalMemory(false));
				Debugger.Launch();
				
				Assert.True(true);
			}
		}


		public void munin_stress_testing_ravendb_500kb_in_filesystem()
		{

			var text = File.ReadAllText(FilePath500KB);

			using (var documentStore = NewDocumentStore("munin", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
					}
				}
				Assert.True(true);
			}
		}

		public void esent_stress_testing_ravendb_500kb_in_filesystem()
		{
			var text = File.ReadAllText(FilePath500KB);

			using (var documentStore = NewDocumentStore("esent", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
					}
				}
				Assert.True(true);
			}
		}

		public void esent_stress_testing_ravendb_500kb_in_filesystem_case2()
		{
			var text = File.ReadAllText(FilePath500KB);

			using (var documentStore = NewDocumentStore("esent", false))
			{
				for (int j = 0; j < 1000; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 10; i++)
						{
							session.Store(new RavenJObject {{"content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
					}
				}
				Assert.True(true);
			}
		}

		public void esent_stress_testing_ravendb_100kb_in_filesystem()
		{
			var text = File.ReadAllText(FilePath100KB);

			using (var documentStore = NewDocumentStore("esent", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
					}
				}
				Assert.True(true);
			}
		}

		public void esent_stress_testing_ravendb_simple_object_in_filesystem()
		{
			const string text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla convallis urna eget enim venenatis condimentum. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Suspendisse eget lectus justo, quis condimentum libero. Cras dictum consequat scelerisque. Pellentesque lectus nisi, porttitor id posuere et, dignissim sed orci. Vivamus sollicitudin gravida massa faucibus feugiat";

			using (var documentStore = NewDocumentStore("esent", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject { { "Content", text }, { "Id", RavenJToken.FromObject(Guid.NewGuid()) } });
						}
						session.SaveChanges();
					}
					Console.WriteLine(j);
				}
				Assert.True(true);
			}
		}

		public void esent_stress_testing_ravendb_100kb_in_filesystem_with_indexing()
		{
			var text = File.ReadAllText(FilePath100KB);

			using (var documentStore = NewDocumentStore("esent", false))
			{
				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
						// Force indexing
						var stored = session.Query<RavenJObject>().Customize(x => x.WaitForNonStaleResults()).ToArray();
						Assert.NotNull(stored);
						Assert.NotEmpty(stored);
					}

				}
				Assert.True(true);
			}
		}


		public void esent_stress_testing_ravendb_100kb_in_filesystem_with_indexing_case2()
		{
			var text = File.ReadAllText(FilePath100KB);

			using (var documentStore = NewDocumentStore("esent", false))
			{

				for (int j = 0; j < 100; j++)
				{
					using (var session = documentStore.OpenSession())
					{
						for (int i = 0; i < 100; i++)
						{
							session.Store(new RavenJObject {{"Content", text}, {"Id", RavenJToken.FromObject(Guid.NewGuid())}});
						}
						session.SaveChanges();
					}
					// Force indexing
					using (var session = documentStore.OpenSession())
					{
						var stored = session.Query<RavenJObject>().Customize(x => x.WaitForNonStaleResults()).ToArray();
						Assert.NotNull(stored);
						Assert.NotEmpty(stored);
					}
				}
				Assert.True(true);
			}

		}
	}
}
