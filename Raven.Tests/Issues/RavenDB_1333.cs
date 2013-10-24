// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1333.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Collections.Concurrent;

	using Raven.Abstractions.Data;
	using Raven.Client.Document;

	using Xunit;

	public class RavenDB_1333 : RavenTest
	{
		internal class Person
		{
			public int Id { get; set; }

			public string FirstName { get; set; }
		}

		internal class User
		{
			public int Id { get; set; }

			public string FirstName { get; set; }
		}

		[Fact]
		public void ForDocumentsInCollectionEmbedded1()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				store.Changes()
					.ForDocumentsInCollection("users")
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsInCollectionEmbedded2()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				store.Changes()
					.ForDocumentsInCollection<Person>()
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("people/1", documentChangeNotification.Id);
				Assert.Equal("People", documentChangeNotification.CollectionName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsInCollectionRemote1()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocumentsInCollection("users");
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(3)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsInCollectionRemote2()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocumentsInCollection<Person>();
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(3)));

				Assert.Equal("people/1", documentChangeNotification.Id);
				Assert.Equal("People", documentChangeNotification.CollectionName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeEmbedded1()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				store.Changes()
					.ForDocumentsOfType(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)))
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeEmbedded2()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				store.Changes()
					.ForDocumentsOfType(typeof(User))
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeEmbedded3()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				store.Changes()
					.ForDocumentsOfType<Person>()
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("people/1", documentChangeNotification.Id);
				Assert.Equal("People", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(Person)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeRemote1()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocumentsOfType(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)));
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeRemote2()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocumentsOfType(typeof(User));
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("users/1", documentChangeNotification.Id);
				Assert.Equal("Users", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(User)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}

		[Fact]
		public void ForDocumentsOfTypeRemote3()
		{
			using (var store = NewDocumentStore())
			{
				var list = new BlockingCollection<DocumentChangeNotification>();

				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var observableWithTask = taskObservable.ForDocumentsOfType<Person>();
				observableWithTask.Task.Wait();
				observableWithTask.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new Person());
					session.Store(new User());
					session.SaveChanges();
				}

				DocumentChangeNotification documentChangeNotification;
				Assert.True(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("people/1", documentChangeNotification.Id);
				Assert.Equal("People", documentChangeNotification.CollectionName);
				Assert.Equal(ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(Person)), documentChangeNotification.TypeName);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);
			}
		}
	}
}