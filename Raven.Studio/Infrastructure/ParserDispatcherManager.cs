using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Parsing.Implementation;

namespace Raven.Studio.Infrastructure
{
    public static class ParserDispatcherManager
    {
        public static void EnsureParserDispatcherIsCreated()
        {
            if (AmbientParseRequestDispatcherProvider.Dispatcher == null)
                AmbientParseRequestDispatcherProvider.Dispatcher = new ThreadedParseRequestDispatcher();
        }
    }
}