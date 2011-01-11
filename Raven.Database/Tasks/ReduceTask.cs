//-----------------------------------------------------------------------
// <copyright file="ReduceTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using log4net;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
	public class ReduceTask : Task
	{
		private readonly static ILog log = LogManager.GetLogger(typeof (ReduceTask));

		public string[] ReduceKeys { get; set; }

		public override bool SupportsMerging
		{
			get
			{
				return ReduceKeys.Length < 128;
			}
		}

	    public override bool TryMerge(Task task)
		{
			var reduceTask = ((ReduceTask)task);
	    	ReduceKeys = ReduceKeys.Concat(reduceTask.ReduceKeys).Distinct().ToArray();
	    	return true;
		}

		public override void Execute(WorkContext context)
		{
            if (ReduceKeys.Length == 0)
                return;

			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(Index);
			if (viewGenerator == null)
				return; // deleted view?
			
			context.TransactionaStorage.Batch(actions =>
			{
				IEnumerable<object> mappedResults = new object[0];
				foreach (var reduceKey in ReduceKeys)
				{
					IEnumerable<object> enumerable = actions.MappedResults.GetMappedResults(Index, reduceKey, MapReduceIndex.ComputeHash(Index, reduceKey))
						.Select(JsonToExpando.Convert);

					mappedResults = mappedResults.Concat(enumerable);
				}
				var sp = Stopwatch.StartNew();
				log.DebugFormat("Starting to read {0} reduce keys for index {1}", ReduceKeys.Length, Index);

				var results = mappedResults.ToArray();

				log.DebugFormat("Read {0} reduce keys in {1} with {2} results for index {3}", ReduceKeys.Length, sp.Elapsed, results.Length, Index);
				sp = Stopwatch.StartNew();
				context.IndexStorage.Reduce(Index, viewGenerator, results, context, actions, ReduceKeys);
				log.DebugFormat("Indexed {0} reduce keys in {1} with {2} results for index {3}", ReduceKeys.Length, sp.Elapsed,
				                results.Length, Index);

			});
		}

		public override Task Clone()
		{
			return new ReduceTask
			{
				Index = Index,
				ReduceKeys = ReduceKeys
			};
		}
	}
}
