using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.QueueSink.Commands;

public sealed class BatchQueueSinkScriptCommand : DocumentMergedTransactionCommand
{
    private readonly List<BlittableJsonReaderObject> _messages;
    private readonly string _script;
    private readonly Logger _logger;
    private readonly List<Exception> _scriptErrors = new();

    public BatchQueueSinkScriptCommand(string script, List<BlittableJsonReaderObject> messages, Logger logger)
    {
        _script = script ?? throw new ArgumentException("Script cannot be null", nameof(script));
        _messages = messages ?? throw new ArgumentException("Messages cannot be null", nameof(messages));
        _logger = logger;
    }

    public int ProcessedSuccessfully { get; private set; }

    /// <summary>
    /// The errors thrown by the script while processing the messages (one per failed message)
    /// </summary>
    public IReadOnlyList<Exception> ScriptErrors => _scriptErrors;

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        ProcessedSuccessfully = 0;
        _scriptErrors.Clear();

        var mainScript = new PatchRequest(_script, PatchRequestType.QueueSink);

        using (context.DocumentDatabase.Scripts.GetScriptRunner(mainScript, readOnly: false, out var documentScript))
        {
            var processed = 0L;

            foreach (var message in _messages)
            {
                try
                {
                    processed++;

                    using (documentScript.Run(context, context, "execute", new object[] {message}))
                    {
                    }

                    ProcessedSuccessfully++;
                }
                catch (Exception e)
                {
                    if (_logger?.IsOperationsEnabled == true)
                        _logger.Operations("Failed to process consumed message by the script.", e);

                    _scriptErrors.Add(e);
                }
            }

            return processed;
        }
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
            return new BatchQueueSinkScriptCommand(Script, Messages, logger: null);
        }
    }
}
