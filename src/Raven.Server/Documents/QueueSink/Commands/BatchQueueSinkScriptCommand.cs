using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Stats;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.QueueSink.Commands;

public sealed class BatchQueueSinkScriptCommand : DocumentMergedTransactionCommand
{
    private readonly List<BlittableJsonReaderObject> _messages;
    private readonly string _script;
    private readonly QueueSinkStatsScope _scriptProcessingScope;
    private readonly QueueSinkProcessStatistics _statistics;
    private readonly Logger _logger;

    public BatchQueueSinkScriptCommand(string script, List<BlittableJsonReaderObject> messages, QueueSinkStatsScope scriptProcessingScope,
        QueueSinkProcessStatistics statistics, Logger logger)
    {
        _messages = messages;
        _script = script;
        _scriptProcessingScope = scriptProcessingScope;
        _statistics = statistics;
        _logger = logger;
    }

    private BatchQueueSinkScriptCommand(string script, List<BlittableJsonReaderObject> messages)
    {
        _messages = messages;
        _script = script;
        _scriptProcessingScope = null;
        _statistics = null;
        _logger = null;
    }

    public int ProcessedSuccessfully { get; private set; }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var mainScript = new PatchRequest(_script, PatchRequestType.QueueSink);

        context.DocumentDatabase.Scripts.GetScriptRunner(mainScript, false, out var documentScript);

        var processed = 0L;

        foreach (var message in _messages)
        {
            try
            {
                processed++;

                using (message)
                using (documentScript.Run(context, context, "execute", new object[] { message }))
                {
                }

                _scriptProcessingScope?.RecordProcessedMessage();
                ProcessedSuccessfully++;
            }
            catch (Exception e)
            {
                if (_logger?.IsOperationsEnabled == true)
                    _logger.Operations("Failed to process consumed message by the script.", e);

                _scriptProcessingScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        return processed;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        var dto = new Dto
        {
            Script = _script,
            Messages = _messages.ToList()
        };

        return dto;
    }

    public class Dto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand>
    {
        public string Script { get; set; }

        public List<BlittableJsonReaderObject> Messages { get; set; }

        public DocumentMergedTransactionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BatchQueueSinkScriptCommand(Script, Messages);
        }
    }
}
