using System;
using System.Linq;
using System.Collections.Generic;
using Jint.Native;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue;

internal class QueueDocumentTransformer : EtlTransformer<QueueItem, QueueWithEvents, EtlStatsScope, EtlPerformanceOperation>
{
    private readonly QueueEtlConfiguration _config;
    private readonly Dictionary<string, QueueWithEvents> _queues;
    private readonly List<EtlQueue> _queuesForScript; //todo djordje: do I need this?
    
    public QueueDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config) 
        : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.QueueEtl), null)
    {
        _config = config;

        var destinationQueues = transformation.GetCollectionsFromScript();

        LoadToDestinations = destinationQueues;

        _queues = new Dictionary<string, QueueWithEvents>(destinationQueues.Length, StringComparer.OrdinalIgnoreCase);
        _queuesForScript = new List<EtlQueue>(destinationQueues.Length);

        for (var i = 0; i < _config.EtlQueues.Count; i++)
        {
            var queue = _config.EtlQueues[i];

            if (destinationQueues.Contains(queue.Name, StringComparer.OrdinalIgnoreCase))
                _queuesForScript.Add(queue);
        }
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        if (DocumentScript == null)
            return;
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new NotSupportedException("Attachments aren't supported by Queue ETL");
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotSupportedException("Counters aren't supported by Queue ETL");
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotSupportedException("Time series aren't supported by Queue ETL");
    }

    protected override string[] LoadToDestinations { get; }
    protected override void LoadToFunction(string queueName, ScriptRunnerResult document)
    {
        if (queueName == null)
            ThrowLoadParameterIsMandatory(nameof(queueName));

        var result = document.TranslateToObject(Context);
        var index = GetOrAdd(queueName);
        index.Inserts.Add(new QueueItem(Current) {TransformationResult = result});
    }

    public override IEnumerable<QueueWithEvents> GetTransformedResults()
    {
        return _queues.Values.ToList();
    }

    public override void Transform(QueueItem item, EtlStatsScope stats, EtlProcessState state)
    {
        if (item.IsDelete == false)
        {
            Current = item;
            DocumentScript.Run(Context, Context, "execute", new object[] {Current.Document}).Dispose();
        }
    }
    
    private QueueWithEvents GetOrAdd(string queueName)
    {
        if (_queues.TryGetValue(queueName, out QueueWithEvents queue) == false)
        {
            var etlQueue = _config.EtlQueues.Find(x => x.Name.Equals(queueName, StringComparison.OrdinalIgnoreCase));

            if (etlQueue == null)
                ThrowQueueNotDefinedInConfig(queueName);

            _queues[queueName] = queue = new QueueWithEvents(etlQueue);
        }

        return queue;
    }

    private static void ThrowQueueNotDefinedInConfig(string queueName)
    {
        throw new InvalidOperationException($"Queue '{queueName}' was not defined in the configuration of Queue ETL task");
    }
}
