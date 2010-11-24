namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System;
    using Caliburn.Micro;
    using Raven.ManagementStudio.Plugin;

    public class ErrorViewModel : Screen, IRavenScreen
    {
        private string message;

        public ErrorViewModel(string message)
        {
            this.DisplayName = "Error";
            this.Message = message;
        }

        public string Message
        {
            get { return this.message; }
            set
            {
                this.message = value;
                this.NotifyOfPropertyChange(() => this.Message);
            }
        }

        #region IRavenScreen Members

        public IRavenScreen ParentRavenScreen
        {
            get { throw new NotImplementedException(); }
        }

        #endregion
    }
}