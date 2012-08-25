using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Faceted;
using Raven.Abstractions.Extensions;
using Raven.Tests.Indexes;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{
			using(var x = new CompiledIndex())
			{
				x.CompileIndexWillTurnEventsToAggregate();
			}
		}

		public class Person
		{
			public string State { get; set; }
		}
		public class Population
		{
			public int Count { get; set; }
			public string State { get; set; }

			public override string ToString()
			{
				return string.Format("Count: {0}, State: {1}", Count, State);
			}
		}
		public class PopulationByState : AbstractIndexCreationTask<Person, Population>
		{
			public PopulationByState()
			{
				Map = people => from person in people
								select new { person.State, Count = 1 };
				Reduce = results => from result in results
									group result by result.State
										into g
										select new { State = g.Key, Count = g.Sum(x => x.Count) };
			}
		}
	}
}
