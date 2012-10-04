namespace Raven.Studio.Models
{
    public class ApplicationState
    {
        public ApplicationState()
        {
            Databases = new PerDatabaseStateCollection();
        }

        public PerDatabaseStateCollection Databases { get; private set; }
    }
}