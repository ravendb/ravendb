using System;

namespace Rhino.DivanDB.Client
{
    public class DocumentStore : IDisposable
    {
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
            database.Dispose();
        }

        public void Initialise()
        {
            database = new DocumentDatabase(Database);
            database.SpinBackgroundWorkers();
            database.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        private DocumentDatabase database;

        public void Delete(Guid id)
        {
            database.Delete(id.ToString());
        }
    }
}