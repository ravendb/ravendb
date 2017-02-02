using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Storage;
using Sparrow.Collections.LockFree;

namespace RachisTests
{
    public class DictionaryStateMachine : IRaftStateMachine
    {
        public long LastAppliedIndex
        {
            get { return Interlocked.Read(ref _lastAppliedIndex); }
            private set { Interlocked.Exchange(ref _lastAppliedIndex, value); }
        }


        public ConcurrentDictionary<string, int> Data = new ConcurrentDictionary<string, int>();

        private long _lastAppliedIndex;

        public void Apply(LogEntry entry)
        {
            if (LastAppliedIndex >= entry.Index)
                throw new InvalidOperationException("Already applied " + entry.Index);

            LastAppliedIndex = entry.Index;
            try
            {
                var dicCommand = Command.FromBytes<DictionaryCommand>(entry.Data);
                dicCommand.Apply(Data);
            }
            catch 
            {
                //TODO:handle this when we switch out of newtonsoft
            }                           
        }

        public void Dispose()
        {
            //nothing to do
        }
    }
}
