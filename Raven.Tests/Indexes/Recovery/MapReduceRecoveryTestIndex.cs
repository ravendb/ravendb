// -----------------------------------------------------------------------
//  <copyright file="RecoveryTestIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Indexes.Recovery
{
	public class MapReduceRecoveryTestIndex : AbstractIndexCreationTask<Recovery>
	{
		public MapReduceRecoveryTestIndex()
		{
			Map = recoveries => from recovery in recoveries select new {recovery.Name, recovery.Number};

			Reduce =
				results =>
				from result in results group result by result.Name into g select new {Name = g.Key, Number = g.Sum(x => x.Number)};
		}
	}
}