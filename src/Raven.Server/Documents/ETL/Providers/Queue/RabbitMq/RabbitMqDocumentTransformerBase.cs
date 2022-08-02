using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

public abstract class RabbitMqDocumentTransformerBase<T, TJsType> : QueueDocumentTransformer<T, RabbitMqItem, TJsType>
    where T : QueueItem
    where TJsType : struct, IJsHandle<TJsType>
{
    protected RabbitMqDocumentTransformerBase(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config) : base(transformation, database, context, config)
    {
    }

    protected override void LoadToFunction(string exchangeName, ScriptRunnerResult<TJsType> document)
    {
        LoadToFunction(exchangeName, document, null, null);
    }

    private void LoadToFunction(string exchangeName, ScriptRunnerResult<TJsType> document, string routingKey, CloudEventAttributes attributes)
    {
        if (exchangeName == null)
            ThrowLoadParameterIsMandatory(nameof(exchangeName));

        var result = document.TranslateToObject(Context);

        var exchange = GetOrAdd(exchangeName);

        exchange.Items.Add(new RabbitMqItem(Current) { TransformationResult = result, RoutingKey = routingKey, Attributes = attributes });
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        EngineHandle.SetGlobalClrCallBack(Transformation.LoadTo, LoadToFunctionTranslatorWithRoutingKeyAndAttributes);

        foreach (var exchangeName in LoadToDestinations)
        {
            if (exchangeName == RabbitMqEtl.DefaultExchange)
                continue;

            var name = Transformation.LoadTo + exchangeName;
            EngineHandle.SetGlobalClrCallBack(name, (_, args) => LoadToFunctionTranslatorWithRoutingKeyAndAttributes(exchangeName, args));
        }
    }

    private TJsType LoadToFunctionTranslatorWithRoutingKeyAndAttributes(TJsType self, TJsType[] args)
    {
        var methodSignature = "loadTo(name, obj, routingKey, attributes)";

        if ((args.Length != 2 && args.Length != 3) && (args.Length != 3 && args.Length != 4))
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with 2, 3 or 4 parameters");

        if (args[0].IsStringEx == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} first argument must be a string");

        if (args[1].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} second argument must be an object");

        string routingKey = null;
        TJsType attributes = default;

        if (args.Length == 3)
        {
            if (args[2].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} third argument ('routingKey') and must be a string");
            routingKey = args[2].AsString;
        }
        
        if (args.Length == 4)
        {
            if (args[3].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'attributes' must be an object");

            attributes = args[3];
        }

        return LoadToFunctionTranslatorWithAttributesInternal(args[0].AsString, args[1], routingKey, attributes);
    }

    private TJsType LoadToFunctionTranslatorWithRoutingKeyAndAttributes(string name, TJsType[] args)
    {
        var methodSignature = $"loadTo{name}(obj, routingKey, attributes)";

        if ((args.Length != 1 && args.Length != 2) && (args.Length != 2 && args.Length != 3))
            ThrowInvalidScriptMethodCall($"{methodSignature} must be called with with 2 or 3 parameters");

        if (args[0].IsObject == false)
            ThrowInvalidScriptMethodCall($"{methodSignature} argument 'obj' must be an object");

        string routingKey = null;
        TJsType attributes = default;

        if (args.Length == 2)
        {
            if (args[1].IsStringEx)
                routingKey = args[1].AsString;
            else if (args[1].IsObject)
                attributes = args[1];
            else
                ThrowInvalidScriptMethodCall($"{methodSignature} second argument can be either the 'routingKey' and must be a string or it can be 'attributes' and must be an object");
        }

        if (args.Length == 3)
        {
            if (args[1].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'routingKey' must be a string");

            if (args[2].IsObject == false)
                ThrowInvalidScriptMethodCall($"{methodSignature} argument 'attributes' must be an object");

            routingKey = args[1].AsString;
            attributes = args[2];

        }

        return LoadToFunctionTranslatorWithAttributesInternal(name, args[0], routingKey, attributes);
    }

    private TJsType LoadToFunctionTranslatorWithAttributesInternal(string name, TJsType obj, string routingKey, TJsType attributes)
    {
        var result = CreateScriptRunnerResult(obj);

        CloudEventAttributes cloudEventAttributes = null;

        if (attributes.IsNull == false)
            cloudEventAttributes = GetCloudEventAttributes(attributes);

        LoadToFunction(name, result, routingKey, cloudEventAttributes);

        return result.Instance;
    }
}
