using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public abstract class KafkaDocumentTransformerBase<T, TJsType> : QueueDocumentTransformer<T, KafkaItem, TJsType>
    where T : QueueItem
    where TJsType : struct, IJsHandle<TJsType>
{
    protected KafkaDocumentTransformerBase(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config) :
        base(transformation, database, context, config)
    {
    }

    protected override void LoadToFunction(string topicName, ScriptRunnerResult<TJsType> document)
    {
        LoadToFunction(topicName, document, null);
    }

    private void LoadToFunction(string topicName, ScriptRunnerResult<TJsType> document, CloudEventAttributes attributes)
    {
        if (topicName == null)
            ThrowLoadParameterIsMandatory(nameof(topicName));

        var result = document.TranslateToObject(Context);

        var topic = GetOrAdd(topicName);

        topic.Items.Add(new KafkaItem(Current) { TransformationResult = result, Attributes = attributes });
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        EngineHandle.SetGlobalClrCallBack(Transformation.LoadTo, LoadToFunctionTranslatorWithAttributes);

        foreach (var queueName in LoadToDestinations)
        {
            var name = Transformation.LoadTo + queueName;

            EngineHandle.SetGlobalClrCallBack(name, (_, args) => LoadToFunctionTranslatorWithAttributes(queueName, args));
        }
    }

    private TJsType LoadToFunctionTranslatorWithAttributes(TJsType self, TJsType[] args)
    {
        var methodSignature = "loadTo(name, obj, attributes)";

        if (args.Length != 2 && args.Length != 3)
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with 2 or 3 parameters");

        if (args[0].IsStringEx == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

        if (args[1].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

        if (args.Length == 3 && args[2].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} third argument must be an object");

        return LoadToFunctionTranslatorWithAttributesInternal(args[0].AsString, args[1], args.Length == 3 ? args[2] : default);
    }

    private TJsType LoadToFunctionTranslatorWithAttributes(string name, TJsType[] args)
    {
        var methodSignature = $"loadTo{name}(obj, attributes)";

        if (args.Length != 1 && args.Length != 2)
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with with 1 or 2 parameters");

        if (args[0].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

        if (args.Length == 2 && args[1].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} argument 'attributes' must be an object");

        return LoadToFunctionTranslatorWithAttributesInternal(name, args[0], args.Length == 2 ? args[1] : default);
    }

    private TJsType LoadToFunctionTranslatorWithAttributesInternal(string name, TJsType obj, TJsType attributes)
    {
        var result = CreateScriptRunnerResult(obj);

        CloudEventAttributes cloudEventAttributes = null;

        if (attributes.IsNull == false)
            cloudEventAttributes = GetCloudEventAttributes(attributes);

        LoadToFunction(name, result, cloudEventAttributes);

        return result.Instance;
    }
}
