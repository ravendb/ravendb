using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.DTC;

namespace Raven.Tryouts
{
	public class Program
	{
		public static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear(); ;
				Console.WriteLine(i);
				using (var x = new UsingDTCForUpdates())
				{
					x.can_update_a_doc_after_inserting_another_within_transaction_scope();
				}
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
