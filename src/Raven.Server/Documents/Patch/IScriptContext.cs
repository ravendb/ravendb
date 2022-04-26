using System;
using System.Threading;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public interface IJavaScriptContext
    {
        Exception LastException { get; set; }
    }
}
