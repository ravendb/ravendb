using System;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Database.Tasks
{
    public class IndexDocumentTask : Task
    {
        private readonly ILog logger = LogManager.GetLogger(typeof (IndexDocumentTask));
        public string Key { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentTask - Key: {0}", Key);
        }

        public override void Execute(WorkContext context)
        {
            context.TransactionaStorage.Batch(actions =>
            {
                var doc = actions.DocumentByKey(Key);
                if (doc == null)
                {
                    actions.Commit();
                    return;
                }

                var json = JsonToExpando.Convert(doc.ToJson());

                foreach (var index in context.IndexDefinitionStorage.IndexNames)
                {
                    var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
                    if (viewGenerator == null)
                        continue; // index was deleted, probably
                    try
                    {
                        logger.DebugFormat("Indexing document: '{0}' for index: {1}", doc.Key, index);

                        var failureRate = actions.GetFailureRate(index);
                        if(failureRate.IsInvalidIndex)
                        {
                            logger.InfoFormat("Skipped indexing document: '{0}' for index: {1} because failure rate is too high: {2}", doc.Key, index, 
                                failureRate.FailureRate);
                            continue;
                        }
                        

                        context.IndexStorage.Index(index, viewGenerator, new[] {json,},
                                                   context, actions);
                    }
                    catch (Exception e)
                    {
                        logger.WarnFormat(e, "Failed to index document '{0}' for index: {1}", doc.Key, index);
                    }
                }
                actions.Commit();
            });
        }
    }
}