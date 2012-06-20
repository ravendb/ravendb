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
using Raven.Studio.Features.Util;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
    public class ShowErrorListWindowCommand : Command
    {
        public override void Execute(object parameter)
        {
            ErrorListWindow.ShowNew();
        }
    }
}
