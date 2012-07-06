using Raven.Imports.SignalR.Hubs;

namespace Raven.Imports.SignalR.Hubs
{
    public class NullJavaScriptMinifier : IJavaScriptMinifier
    {
        public static readonly NullJavaScriptMinifier Instance = new NullJavaScriptMinifier();

        public string Minify(string source)
        {
            return source;
        }
    }
}
