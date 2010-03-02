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

        public string Database { get; set; }

        public DocumentConvention Conventions { get; set; }

        public DocumentSession OpenSession()
        {
            return new DocumentSession(this, database);
        }

        public void Dispose()
        {
            var disposable = database as IDisposable;
            if (disposable != null)
                disposable.Dispose();
        }

        public void Initialise()
        {
            if (String.IsNullOrEmpty(localhost))
            {
                var embeddedDatabase = new DocumentDatabase(Database);
                embeddedDatabase.SpinBackgroundWorkers();
                database = embeddedDatabase;
            }
            else
            {
                database = new ServerClient(localhost, port);
            }
            //NOTE: this should be done contitionally, index creation is expensive
            database.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        private IDatabaseCommands database;

        public void Delete(Guid id)
        {
            database.Delete(id.ToString());
        }
    }
}