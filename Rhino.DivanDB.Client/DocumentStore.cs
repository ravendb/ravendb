using System;

namespace Rhino.DivanDB.Client
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
            var embeddedDatabase = (DocumentDatabase)database;
            if (embeddedDatabase != null)
                embeddedDatabase.Dispose();
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
                database = new ServerClient(localhost)
            }

            database.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        private IDatabaseCommands database;

        public void Delete(Guid id)
        {
            database.Delete(id.ToString());
        }
    }
}