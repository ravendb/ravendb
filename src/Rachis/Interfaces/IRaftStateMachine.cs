using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Storage;

namespace Rachis.Interfaces
{
    public interface IRaftStateMachine : IDisposable
    {

        long LastAppliedIndex { get; }

        void Apply(LogEntry entry);

    }
}
