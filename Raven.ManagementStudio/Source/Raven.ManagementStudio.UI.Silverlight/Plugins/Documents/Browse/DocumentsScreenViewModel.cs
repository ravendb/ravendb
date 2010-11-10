namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse
{
    using Caliburn.Micro;
    using Plugin;

    public class DocumentsScreenViewModel : Screen, IRavenScreen
    {
        public DocumentsScreenViewModel()
        {
            this.DisplayName = "Browse";
        }

        public void ChangeView(object view)
        { 
        }
    }
}