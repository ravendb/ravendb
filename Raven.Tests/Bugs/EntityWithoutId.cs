//-----------------------------------------------------------------------
// <copyright file="EntityWithoutId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Bugs
{
	public class EntityWithoutId : RavenTest
	{
		[Fact]
		public void CanBeSaved()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = new User { Name = "Ayende #" + i };
						s.Store(clone);
					}
					s.SaveChanges();
				}

				
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						Assert.Equal("Ayende #" + i, s.Load<User>("users/"+(i+1)).Name);
					}
				}
			}
		}

		[Fact]
		public void CanBeUpdated()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = new User { Name = "Ayende #" + i };
						s.Store(clone);
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = s.Load<User>("users/" + (i + 1));
						clone.Name = "Rahien #" + i;
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						Assert.Equal("Rahien #" + i, s.Load<User>("users/" + (i + 1)).Name);
					}
				}
			}
		}

		[Fact]
		public void CanBeDeleted()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = new User { Name = "Ayende #" + i };
						s.Store(clone);
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = s.Load<User>("users/" + (i + 1));
						s.Delete(clone);
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						Assert.Null(s.Load<User>("users/" + (i + 1)));
					}
				}
			}
		}

		[Fact]
		public void CanGetId()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = new User { Name = "Ayende #" + i };
						s.Store(clone);
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						var clone = s.Load<User>("users/" + (i + 1));
						Assert.Equal("users/" + (i + 1), s.Advanced.GetDocumentId(clone));
					}
				}
			}
		}
		public class User
		{
			public string Name { get; set; }
		}
	}
}
