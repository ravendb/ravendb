using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class Nullable : RavenTest
    {
        public class Widget
        {
            public string Id { get; set; }
            public string Group {get; set;}
            public long? Long { get; set; }
        }

        public class CountResult
        {
            public string Group {get; set;}
        	public int Count {get; set;}
        }
        
        public class Index : AbstractIndexCreationTask<Widget, CountResult>
        {
            public Index()
            {
                Map = widgets => from w in widgets
                                 where w.Long != null
                                 select new CountResult()
                                 {
                                     Group = w.Group,
                                     Count = 1
                                 };

                Reduce = results => from r in results 
                                    group r by r.Group into g
                                    select new CountResult()
                                    {
                                        Group = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

		[Fact]
		public void NoIndexingErrors()
		{
			using (var store = NewDocumentStore())
			{
        		using (var session = store.OpenSession())
				{
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store( new Widget() { Long = null } );
                    } 
                    
                    session.Store( new Widget() { Long = 1, Group = "A" } );
                    session.Store(new Widget() { Long = 2, Group = "A" });
                    session.Store(new Widget() { Long = 3, Group = "B" });
                    session.Store(new Widget() { Long = 4, Group = "A" });

					session.SaveChanges();
				}

                store.ExecuteIndex( new Index() );

				WaitForIndexing(store);

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);

                using (var session = store.OpenSession())
                {
                    // throws a IndexDisabledException
                    var counts = session.Query<CountResult, Index>().Customize( x => x.WaitForNonStaleResults() ).ToArray();
                    Assert.NotEmpty(counts );
                }

			}
		}

    }
}
