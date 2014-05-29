using Raven.Client.Connection.Profiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IAsyncFilesCommands : IDisposable, IHoldProfilingInformation
    {
    }
}
