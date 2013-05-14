// -----------------------------------------------------------------------
//  <copyright file="Animals_Stats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bundles.ScriptedIndexResults
{
	public class Animals_Stats : AbstractIndexCreationTask<Animal, Animals_Stats.Result>
	{
		public class Result
		{
			public string Type { get; set; }
			public int Count { get; set; }
		}
		public Animals_Stats()
		{
			Map = animals =>
			      from animal in animals
			      select new
			      {
				      animal.Type,
				      Count = 1
			      };
			Reduce = animals =>
			         from result in animals
			         group result by result.Type
			         into g
			         select new
			         {
				         Type = g.Key,
				         Count = g.Sum(x => x.Count)
			         };

		}
	}
}