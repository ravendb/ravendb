using Raven.Abstractions.Smuggler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Smuggler
{
    public abstract class AbstractSmugglerOperation<T> : IDisposable where T : SmugglerOptions
    {
        protected T Parameters;

        protected AbstractSmugglerOperation( T parameters )
        {
            this.Parameters = parameters;
        }

        public abstract bool InitSmuggler();

        public void WaitForSmuggler()
        {
            throw new NotImplementedException();
        }

        public abstract void Dispose();
    }
}
