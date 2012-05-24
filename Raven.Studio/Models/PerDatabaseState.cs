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

namespace Raven.Studio.Models
{
    public class PerDatabaseState
    {
        public string DatabaseName { get; private set; }

        public PerDatabaseState(string databaseName)
        {
            DatabaseName = databaseName;
            DocumentViewState = new DocumentViewStateStore();
        }

        public DocumentViewStateStore DocumentViewState { get; private set; }
    }
}
