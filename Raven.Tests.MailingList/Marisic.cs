// -----------------------------------------------------------------------
//  <copyright file="Marisic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Marisic : RavenTest
	{
		[Fact]
		public void StartsWith_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.LoadStartingWith<object>(keyPrefix: "user/");
					Assert.Equal(objects.Length, names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.LoadStartingWith<object>(keyPrefix: "user/");
					Assert.Equal(objects.Length, names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Esent_Embedded()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.LoadStartingWith<object>(keyPrefix: "user/");
					Assert.Equal(objects.Length, names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Esent_Remote()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var objects = session.Advanced.LoadStartingWith<object>(keyPrefix: "user/");
					Assert.Equal(objects.Length, names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Embedded_Async()
		{
			using (var store = NewDocumentStore())
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenAsyncSession())
				{
					var objects = session.Advanced.LoadStartingWithAsync<object>(keyPrefix: "user/", matches: null).Result;
					Assert.Equal(objects.Count(), names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Remote_Async()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenAsyncSession())
				{
					var objects = session.Advanced.LoadStartingWithAsync<object>(keyPrefix: "user/", matches: null).Result;
					Assert.Equal(objects.Count(), names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Esent_Embedded_Async()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenAsyncSession())
				{
					var objects = session.Advanced.LoadStartingWithAsync<object>(keyPrefix: "user/", matches: null).Result;
					Assert.Equal(objects.Count(), names.Length);
				}
			}
		}

		[Fact]
		public void StartsWith_Esent_Remote_Async()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
				var names = new[]
				{
					"user/aaaaaa/foo a/bar",
					"user/aaaaaa/foo b/bar",
					"user/aaaaaa/foo-c/bar",
					"user/aaaaaa/foo d/bar",
					"user/aaaaaa/foo e/bar",
					"user/bazbar/foo1/baz",
					"user/bazbar/foo2/baz",
					"user/bazbar/foo3/baz",
					"user/bazbar/foo4/baz",
					"user/bazbar/foo5/baz"
				};

				using (var session = store.OpenSession())
				{
					foreach (var name in names)
					{
						session.Store(new object(), name);
					}
					session.SaveChanges();
				}

				using (var session = store.OpenAsyncSession())
				{
					var objects = session.Advanced.LoadStartingWithAsync<object>(keyPrefix: "user/", matches: null).Result;
					Assert.Equal(objects.Count(), names.Length);
				}
			}
		}
	}
}