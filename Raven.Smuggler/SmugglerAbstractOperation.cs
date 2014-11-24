using Raven.Abstractions.Smuggler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Smuggler
{
    public abstract class SmugglerAbstractOperation<T> : IDisposable where T : SmugglerOptions
    {
        protected T Parameters;

        protected SmugglerAbstractOperation( T parameters )
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
