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
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class SettingsSectionModel : ViewModel
    {
        public string SectionName { get; protected set; }

        public virtual void LoadFor(DatabaseDocument document)
        {
        }
    }
}
