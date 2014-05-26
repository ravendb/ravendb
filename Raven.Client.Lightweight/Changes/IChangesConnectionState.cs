using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    public interface IChangesConnectionState
    {
        Task Task { get; }

        void Inc();
        void Dec();

        void Error(Exception e);
    }
}
