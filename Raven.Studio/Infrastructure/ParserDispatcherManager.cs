using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Parsing.Implementation;

namespace Raven.Studio.Infrastructure
{
    public static class ParserDispatcherManager
    {
        public static void EnsureParserDispatcherIsCreated()
        {
            if (AmbientParseRequestDispatcherProvider.Dispatcher == null)
            {
                AmbientParseRequestDispatcherProvider.Dispatcher = new ThreadedParseRequestDispatcher();
            }
        }
    }
}
