namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From16 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            return From15.UpdateCertificatesTableInternal(step);
        }
    }
}
