using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Interop;
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

        DocumentScript.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadTo, LoadToFunctionTranslatorWithOptions));

        foreach (var queueName in LoadToDestinations)
        {
            var name = Transformation.LoadTo + queueName;
            DocumentScript.ScriptEngine.SetValue(name, new ClrFunctionInstance(DocumentScript.ScriptEngine, name,
                (self, args) => LoadToFunctionTranslatorWithOptions(queueName, args)));
        }
    }

    private JsValue LoadToFunctionTranslatorWithOptions(JsValue self, JsValue[] args)
    {
        var methodSignature = "loadTo(name, obj, options)";

        if (args.Length != 2 && args.Length != 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with 2 or 3 parameters");

        if (args[0].IsString() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

        if (args[1].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

        if (args.Length == 3 && args[2].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

        return LoadToFunctionTranslatorWithOptionsInternal(args[0].AsString(), args[1].AsObject(), args.Length == 3 ? args[2].AsObject() : null);
    }

    private JsValue LoadToFunctionTranslatorWithOptions(string name, JsValue[] args)
    {
        var methodSignature = $"loadTo{name}(obj, options)";

        if (args.Length != 1 && args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with with 1 or 2 parameters");

        if (args[0].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

        if (args.Length == 2 && args[1].IsObject() == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} argument 'options' must be an object");

        return LoadToFunctionTranslatorWithOptionsInternal(name, args[0].AsObject(), args.Length == 2 ? args[1].AsObject() : null);
    }

    private JsValue LoadToFunctionTranslatorWithOptionsInternal(string name, ObjectInstance obj, ObjectInstance options)
    {
        var result = new ScriptRunnerResult(DocumentScript, obj);

        QueueLoadOptions loadOptions = null;

        if (options != null)
        {
            loadOptions = new QueueLoadOptions();

            if (TryGetOptionValue(nameof(QueueLoadOptions.Id), out var messageId))
                loadOptions.Id = messageId;

            if (TryGetOptionValue(nameof(QueueLoadOptions.Type), out var type))
                loadOptions.Type = type;

            if (TryGetOptionValue(nameof(QueueLoadOptions.Source), out var source))
                loadOptions.Source = source;

            if (TryGetOptionValue(nameof(QueueLoadOptions.PartitionKey), out var partitionKey))
                loadOptions.PartitionKey = partitionKey;

            if (TryGetOptionValue(nameof(QueueLoadOptions.RoutingKey), out var routingKey))
                loadOptions.RoutingKey = routingKey;
        }

        LoadToFunction(name, result, loadOptions);
        return result.Instance;

        bool TryGetOptionValue(string optionName, out string value)
        {
            var optionValue = options.GetOwnProperty(optionName).Value;

            if (optionValue != null && optionValue.IsNull() == false && optionValue.IsUndefined() == false)
            {
                value = optionValue.AsString();
                return true;
            }

            value = null;
            return false;
        }
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
        LoadToFunction(queueName, document, null);
    }

    private void LoadToFunction(string queueName, ScriptRunnerResult document, QueueLoadOptions options)
    {
        if (queueName == null)
            ThrowLoadParameterIsMandatory(nameof(queueName));

        var result = document.TranslateToObject(Context);

        var queue = GetOrAdd(queueName);
        queue.Inserts.Add(new QueueItem(Current) { TransformationResult = result, Options = options });
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
