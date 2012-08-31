using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class QueryProvider : RavenTest
	{
		[Fact]
		public void CanCreateQuery()
		{
			using (var store = NewDocumentStore())
			{
				
				store.Initialize();

				new MyIdx().Execute(store);

				var values = new string[] { "AAA", "CCC", "BAA", "BBB", "ABB" };
				var expected = new string[] { "AAA", "ABB", "BAA", "BBB", "CCC" };

				using (var s = store.OpenSession())
				{
					foreach (string v in values)
						s.Store(new MyDoc { Foo = v });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					IRavenQueryable<MyDoc> query = s.Query<MyDoc>("MyIdx").Customize(x=>x.WaitForNonStaleResults());

					Expression
							exp = query.Expression,
							param = Expression.Parameter(typeof(MyDoc)),
							body = Expression.PropertyOrField(param, "Foo");

					exp =
							Expression.Call(
									typeof(Queryable),
									"OrderBy",
									new Type[] { query.ElementType, body.Type },
									exp, Expression.Quote(Expression.Lambda(body,
											new ParameterExpression[] {
															   Expression.Parameter(query.ElementType, "") })));

					query = (IRavenQueryable<MyDoc>)query.Provider.CreateQuery(exp);
					var res = query.ToArray();

					var strings = res.Select(a => a.Foo)
						.ToArray();
					Assert.True(strings
							.SequenceEqual(expected));
				}
			}
		}

		public class MyDoc
		{
			public string Foo { get; set; }
		}

		public class MyIdx : AbstractIndexCreationTask<MyDoc>
		{
			public MyIdx()
			{
				Map = d => d.Select(x => new { x.Foo });
			}
		}

	}

}
