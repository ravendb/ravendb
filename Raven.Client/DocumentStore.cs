using System;
using Raven.Database;

namespace Raven.Client
{
    public class DocumentStore : IDisposable
    {
        private readonly string localhost;
        private readonly int port;

        public DocumentStore(string localhost, int port) : this()
        {
            this.localhost = localhost;
            this.port = port;
        }

        public DocumentStore()
        {
            Conventions = new DocumentConvention();
        }

        public string DataDirectory { get; set; }

        public DocumentConvention Conventions { get; set; }

        public DocumentSession OpenSession()
        {
            return new DocumentSession(this, DatabaseCommands);
        }

        public void Dispose()
        {
            var disposable = DatabaseCommands as IDisposable;
            if (disposable != null)
                disposable.Dispose();
        }

        public void Initialise()
        {
            if (String.IsNullOrEmpty(localhost))
            {
                var embeddedDatabase = new DocumentDatabase(new RavenConfiguration { DataDirectory = DataDirectory });
                embeddedDatabase.SpinBackgroundWorkers();
                DatabaseCommands = embeddedDatabase;
            }
            else
            {
                DatabaseCommands = new ServerClient(localhost, port);
            }
            //NOTE: this should be done contitionally, index creation is expensive
            DatabaseCommands.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        public IDatabaseCommands DatabaseCommands;

        public void Delete(Guid id)
        {
            DatabaseCommands.Delete(id.ToString(), null);
        }
    }
}