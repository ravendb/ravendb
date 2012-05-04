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

namespace Raven.Studio.Features.Documents
{
    public class ColumnDefinition
    {
        public string Header { get; set; }

        /// <summary>
        /// The binding is a property path relative to a JsonDocument, e.g. DataAsJson[Title]
        /// </summary>
        public string Binding { get; set; }

        public string DefaultWidth { get; set; }
    }
}
