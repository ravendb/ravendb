using System;

namespace Raven.Server.Documents.Patch
{
    public interface IJavaScriptContext
    {
        Exception LastException { get; set; }
    }
}
