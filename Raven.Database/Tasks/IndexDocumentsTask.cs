using System;
using System.Linq;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
	public class IndexDocumentsTask : Task
	{
		private readonly ILog logger = LogManager.GetLogger(typeof (IndexDocumentsTask));
		public string[] Keys { get; set; }

		public override string ToString()
		{
			return string.Format("IndexDocumentsTask - Keys: {0}", string.Join(", ", Keys));
		}

		public override bool SupportsMerging
		{
			get
			{
				return Keys.Length < 128;
			}
		}

		public override bool TryMerge(Task task)
		{
			var indexDocumentTask = ((IndexDocumentsTask)task);
			Keys = Keys.Union(indexDocumentTask.Keys).ToArray();
			return true;
		}

		public override void Execute(WorkContext context)
		{
			context.TransactionaStorage.Batch(actions =>
			{
				var jsonDocuments = Keys.Select(key => actions.DocumentByKey(key, null))
					.Select(x => JsonToExpando.Convert(x.ToJson()))
					.ToArray();

				var keysAsString = string.Join(", ", Keys);
				foreach (var index in context.IndexDefinitionStorage.IndexNames)
				{
					var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
					if (viewGenerator == null)
						continue; // index was deleted, probably
					try
					{
						logger.DebugFormat("Indexing documents: [{0}] for index: {1}", keysAsString, index);

						var failureRate = actions.GetFailureRate(index);
						if (failureRate.IsInvalidIndex)
						{
							logger.InfoFormat("Skipped indexing documents: [{0}] for index: {1} because failure rate is too high: {2}",
							                  keysAsString, index,
							                  failureRate.FailureRate);
							continue;
						}


						context.IndexStorage.Index(index, viewGenerator, jsonDocuments,
						                           context, actions);
					}
					catch (Exception e)
					{
						logger.WarnFormat(e, "Failed to index document  [{0}] for index: {1}", keysAsString, index);
					}
				}
				actions.Commit();
			});
		}
	}
}