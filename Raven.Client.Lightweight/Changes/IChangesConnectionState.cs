using System;
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
