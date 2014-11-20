// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2909.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2909 : RavenTest
	{
		[Fact]
		public async Task ThisIsMyTest_Remote()
		{
			using (IDocumentStore store = NewRemoteDocumentStore())
			{
				Console.WriteLine("Fill Database");
				FillDatabase(store);

				Console.WriteLine("Create Index");
				new TestObject_Index().Execute(store);

				Console.WriteLine("Create Transformer");
				new TestObject_Transformer().Execute(store);

				WaitForIndexing(store);

				Console.WriteLine("Sync Session –> Transform");
				TransformResult(store);

				Console.WriteLine("Async Session –> Transform");
				//Fails
				await TransformResultAsync(store);
			}
		}

		[Fact]
		public async Task ThisIsMyTest_Embedded()
		{
			using (IDocumentStore store = NewDocumentStore())
			{
				Console.WriteLine("Fill Database");
				FillDatabase(store);

				Console.WriteLine("Create Index");
				new TestObject_Index().Execute(store);

				Console.WriteLine("Create Transformer");
				new TestObject_Transformer().Execute(store);

				WaitForIndexing(store);

				Console.WriteLine("Sync Session –> Transform");
				TransformResult(store);

				Console.WriteLine("Async Session –> Transform");
				//Fails
				await TransformResultAsync(store);
			}
		}

		public static void FillDatabase(IDocumentStore store)
		{
			Dictionary<string, string> namedict = new Dictionary<string, string>();
			namedict.Add("de", "Franz");
			namedict.Add("it", "Francesco");
			var firstobject = new TestObject() { Id = "Franz18", Age = 18, Hobbies = new List<string>() { "Soccer", "Volleyball" }, Name = namedict };

			Dictionary<string, string> namedict2 = new Dictionary<string, string>();
			namedict2.Add("de", "Anton");
			namedict2.Add("it", "Antonio");
			var secondobject = new TestObject() { Id = "Anton20", Age = 20, Hobbies = new List<string>() { "Soccer", "Volleyball", "Skiing" }, Name = namedict2 };


			using (var session = store.OpenSession())
			{
				session.Store(firstobject);
				session.Store(secondobject);

				session.SaveChanges();
			}
		}

		public static void TransformResult(IDocumentStore store)
		{
			var hobbieslist = new List<string>() { "Soccer", "Volleyball" };

			using (var session = store.OpenSession())
			{
				var result = session.Query<TestObject, TestObject_Index>()
					.Where(x => x.Hobbies.In(hobbieslist))
					.AddQueryInput("lang", "de")
					.TransformWith<TestObject_Transformer, TestObjectReduced>()
					.ToList();

				Console.WriteLine(result.Count + " objekcs found & Transformed");
			}
		}

		public static async Task TransformResultAsync(IDocumentStore store)
		{
			var hobbieslist = new List<string>() { "Soccer", "Volleyball" };

			using (var session = store.OpenAsyncSession())
			{
				var result = await session.Query<TestObject, TestObject_Index>()
					.Where(x => x.Hobbies.In(hobbieslist))
					.AddQueryInput("lang", "de")
					.TransformWith<TestObject_Transformer, TestObjectReduced>()
					.ToListAsync();

				Console.WriteLine(result.Count + " objects found & Transformed");
			}
		}
	}

	public class TestObject
	{
		public TestObject()
		{
			Name = new Dictionary<string, string>();
		}

		public string Id { get; set; }
		public int Age { get; set; }
		public IDictionary<string, string> Name { get; set; }
		public ICollection<string> Hobbies { get; set; }
	}

	public class TestObjectReduced
	{
		public string Id { get; set; }
		public string Name { get; set; }
	}

	public class TestObject_Index : AbstractIndexCreationTask<TestObject>
	{
		public TestObject_Index()
		{
			Map = docs => from doc in docs
						  select new
						  {
							  doc.Hobbies
						  };
		}
	}

	public class TestObject_Transformer : AbstractTransformerCreationTask<TestObject>
	{
		public TestObject_Transformer()
		{
			TransformResults = activities => from activity in activities
											 let language = this.Query("lang").Value<String>()
											 select new TestObjectReduced
											 {
												 Id = activity.Id,
												 Name = activity.Name[language]
											 };
		}
	}
}