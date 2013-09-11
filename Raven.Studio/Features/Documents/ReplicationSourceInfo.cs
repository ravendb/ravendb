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
    public class ReplicationSourceInfo
    {
        public ReplicationSourceInfo(string url)
        {
            Url = url;
            var indexOfDatabases = url.IndexOf("databases/", StringComparison.OrdinalIgnoreCase);
            if (indexOfDatabases > 0)
            {
                Name = url.Substring(indexOfDatabases + 10);
            }
            else
            {
                Name = "";
            }
        }

        public string Url { get; private set; }

        public string Name { get; private set; }
    }
}
