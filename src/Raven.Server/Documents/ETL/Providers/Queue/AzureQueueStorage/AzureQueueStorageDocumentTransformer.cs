using Jint.Runtime.Interop;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.AzureQueueStorage;

public sealed class AzureQueueStorageDocumentTransformer<T> : QueueDocumentTransformer<T, AzureQueueStorageItem>
    where T : QueueItem
{
    public AzureQueueStorageDocumentTransformer(Transformation transformation, DocumentDatabase database,
        DocumentsOperationContext context, QueueEtlConfiguration config) : base(transformation, database, context,
        config)
    {
    }

    protected override void LoadToFunction(string queueName, ScriptRunnerResult document)
    {
        LoadToFunction(queueName, document, null);
    }

    protected override void LoadToFunction(string queueName, ScriptRunnerResult document, CloudEventAttributes attributes)
    {
        if (queueName == null)
            ThrowLoadParameterIsMandatory(nameof(queueName));

        var result = document.TranslateToObject(Context);

        var queue = GetOrAdd(queueName);

        queue.Items.Add(new AzureQueueStorageItem(Current) { TransformationResult = result, Attributes = attributes });
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        DocumentScript.ScriptEngine.SetValue(Transformation.LoadTo,
            new ClrFunction(DocumentScript.ScriptEngine, Transformation.LoadTo,
                LoadToFunctionTranslatorWithAttributes));

        foreach (var queueName in LoadToDestinations)
        {
            var name = Transformation.LoadTo + queueName;

            DocumentScript.ScriptEngine.SetValue(name, new ClrFunction(DocumentScript.ScriptEngine, name,
                (self, args) => LoadToFunctionTranslatorWithAttributes(queueName, args)));
        }
    }
}
