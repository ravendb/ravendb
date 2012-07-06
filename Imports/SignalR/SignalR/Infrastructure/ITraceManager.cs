using System.Diagnostics;

namespace Raven.Imports.SignalR.Infrastructure
{
    public interface ITraceManager
    {
        SourceSwitch Switch { get; }
        TraceSource this[string name] { get; }
    }
}
