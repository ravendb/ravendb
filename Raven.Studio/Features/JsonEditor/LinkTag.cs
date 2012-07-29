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
using ActiproSoftware.Text.Tagging;

namespace Raven.Studio.Features.JsonEditor
{
    public enum LinkTagNavigationType
    {
        Document,
        ExternalUrl,
    }

    public class LinkTag : ITag
    {
        public LinkTagNavigationType NavigationType { get; set; }
        public string Url { get; set; }
    }
}
