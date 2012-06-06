using System;
using System.Collections.Generic;
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
    public class PriorityProperties
    {
        private IList<string> propertyNamePatterns;

        public IList<string> PropertyNamePatterns
        {
            get { return propertyNamePatterns ?? (propertyNamePatterns = new List<string>()); }
            set { propertyNamePatterns = value; }
        }
    }
}
