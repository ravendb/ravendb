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
using Raven.Database.Storage;

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
				var itemsToFind = ReduceKeys
					.Select(reduceKey => new GetMappedResultsParams(Index, reduceKey, MapReduceIndex.ComputeHash(Index, reduceKey)))
					.OrderBy(x=>x.ViewAndReduceKeyHashed, new ByteComparer())
					.ToArray();
				var mappedResults = actions.MappedResults.GetMappedResults(itemsToFind)
					.Select(JsonToExpando.Convert);
				
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

		public class ByteComparer : IComparer<byte[]>
		{
			public int Compare(byte[] x, byte[] y)
			{
				if (x.Length != y.Length)
					return x.Length.CompareTo(y.Length);

				for (int i = 0; i < x.Length; i++)
				{
					if (x[i] != y[i])
						return x[i].CompareTo(y[i]);
				}
				return 0;
			}
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
