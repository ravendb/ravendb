
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	public class Foo
	{
		public string Bar { get; set; }
	}

	public class Foo2 : Foo
	{
	}

	public class SimpleReduceResult
	{
		public string Bar { get; set; }
		public long Count { get; set; }
	}

	public class SimpleMRIndex : AbstractIndexCreationTask<Foo,SimpleReduceResult>
	{
		public SimpleMRIndex()
		{
			Map = foos => from foo in foos
						  select new
						  {
							  Bar = foo.Bar,
							  Count = 1L
						  };

			Reduce = results => from result in results
								group result by result.Bar
									into g
									select new
									{
										Bar = g.Key,
										Count = g.Sum(c => c.Count)
									};
		}
	}

	public class MyIndex : AbstractMultiMapIndexCreationTask
	{
		public MyIndex()
		{
			AddMapForAll<Foo>(foos => from foo in foos select new {foo.Bar});
		}

		private void AddMapForAll<T>(Expression<Func<IEnumerable<T>, IEnumerable>> expr)
		{
			AddMap(expr); // base class

			// child classes
			var children = typeof(T).Assembly.GetTypes().Where(x=>x.IsSubclassOf(typeof(T)));
			var addMapGeneric = GetType().GetMethod("AddMap", BindingFlags.Instance|BindingFlags.NonPublic);
			foreach (var child in children)
			{
				var genericEnumerable = typeof(IEnumerable<>).MakeGenericType(child);
				var delegateType = typeof(Func<,>).MakeGenericType(genericEnumerable, typeof(IEnumerable));
				var lambdaExpression = Expression.Lambda(delegateType,expr.Body, Expression.Parameter(genericEnumerable, expr.Parameters[0].Name));
				addMapGeneric.MakeGenericMethod(child).Invoke(this, new[] { lambdaExpression });
			}
		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			var myIndex = new MyIndex();
			myIndex.Conventions = new DocumentConvention();
			Console.WriteLine(myIndex.CreateIndexDefinition().ToString());

			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				
				var sp = Stopwatch.StartNew();
				for (int i = 0; i < 5000; i++)
				{
					using (var session = store.OpenSession())
					{
						for (int j = 0; j < 100; j++)
						{
							session.Store(new Foo { Bar = "IamBar" });
						}
						session.SaveChanges();
					}
					if(i % 100 == 0)
					{
						Console.Write(".");
					}
				}

				Console.Clear();
				Console.WriteLine("Wrote 500,000 docs in " + sp.Elapsed);
				Console.WriteLine("Done inserting data");

				sp.Restart();
				new SimpleMRIndex().Execute(store);
				Console.WriteLine("indexing...");

				while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
				{
					Console.Write("\r{0:#,#} - {1:#,#}",store.DatabaseCommands.GetStatistics().Indexes[0].IndexingAttempts, store.DatabaseCommands.GetStatistics().Indexes[0].ReduceIndexingAttempts);
					Thread.Sleep(100);
				}
				Console.WriteLine();
				Console.WriteLine("Indexed 500,000 docs in " + sp.Elapsed);

				Console.ReadLine();
			}
		}
	}
}