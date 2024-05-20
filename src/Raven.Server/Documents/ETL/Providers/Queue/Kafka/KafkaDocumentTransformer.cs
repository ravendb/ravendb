using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public sealed class KafkaDocumentTransformer<T> : QueueDocumentTransformer<T, KafkaItem>
    where T : QueueItem
{
    public KafkaDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, QueueEtlConfiguration config) : base(transformation, database, context, config)
    {
    }

    protected override void LoadToFunction(string topicName, ScriptRunnerResult document)
    {
        LoadToFunction(topicName, document, null);
    }

    protected override void LoadToFunction(string topicName, ScriptRunnerResult document, CloudEventAttributes attributes)
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

        DocumentScript.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunction(DocumentScript.ScriptEngine, Transformation.LoadTo, LoadToFunctionTranslatorWithAttributes));

        foreach (var queueName in LoadToDestinations)
        {
            var name = Transformation.LoadTo + queueName;

            DocumentScript.ScriptEngine.SetValue(name, new ClrFunction(DocumentScript.ScriptEngine, name,
                (self, args) => LoadToFunctionTranslatorWithAttributes(queueName, args)));
        }
    }
}
