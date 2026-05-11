using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal sealed class AiDebugTraceCollector
{
    private readonly List<AiDebugTrace> _traces;

    public bool Enabled => _traces != null;

    public AiDebugTraceCollector(bool enabled)
    {
        if (enabled)
            _traces = new List<AiDebugTrace>();
    }

    public AiDebugTrace CreateTrace()
    {
        if (_traces == null)
            return null;

        var t = new AiDebugTrace();
        _traces.Add(t);
        return t;
    }

    public async Task PersistAsync(ConversationDocument document, DocumentDatabase database)
    {
        if (_traces == null || _traces.Count == 0)
            return;

        try
        {
            await database.TxMerger.Enqueue(new PutConversationDebugCommand(_traces, document, database));
        }
        catch (Exception e)
        {
            // Swallow only the secondary debug-trace persistence failure so the original
            // provider/model exception (if any) is preserved and propagated to the caller.
            var logger = database.Loggers.GetLogger<AiDebugTraceCollector>();
            if (logger.IsDebugEnabled)
                logger.Debug($"Failed to persist {_traces.Count} debug trace(s) for conversation '{document.Id}'.", e);
        }
    }
}
