using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal sealed class AiDebugTraceCollector
{
    private readonly List<AiDebugTrace> _traces;
    private readonly DocumentDatabase _database;
    private readonly RavenLogger _logger;

    public bool Enabled => _traces != null;

    public AiDebugTraceCollector(bool enabled, DocumentDatabase database)
    {
        if (enabled == false)
            return;

        _traces = new List<AiDebugTrace>();
        _database = database;
        _logger = database.Loggers.GetLogger<AiDebugTraceCollector>();
    }

    public AiDebugTrace CreateTrace()
    {
        if (_traces == null)
            return null;

        var t = new AiDebugTrace();
        _traces.Add(t);
        return t;
    }

    public async Task PersistAsync(ConversationDocument document)
    {
        if (_traces == null || _traces.Count == 0)
            return;

        try
        {
            await _database.TxMerger.Enqueue(new PutConversationDebugCommand(_traces, document, _database));
        }
        catch (Exception e)
        {
            // Swallow only the secondary debug-trace persistence failure so the original
            // provider/model exception (if any) is preserved and propagated to the caller.
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Failed to persist {_traces.Count} debug trace(s) for conversation '{document.Id}'.", e);
        }
    }
}
