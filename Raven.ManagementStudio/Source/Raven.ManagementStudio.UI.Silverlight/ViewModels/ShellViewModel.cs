using Raven.ManagementStudio.UI.Silverlight.ViewModels.Interfaces;
using System.ComponentModel.Composition;
using Caliburn.Micro;

namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    [Export(typeof(IShell))]
    public class ShellViewModel : Screen, IShell
    {
        private string title;
        public string Title
        {
            get { return title; }
            set
            {
                if (title != value)
                {
                    title = value;
                    RaisePropertyChangedEventImmediately("Title");
                }
            }
        }

        public ShellViewModel()
        {
            Title = "Hello Caliburn.Micro";
        }
    }
}
