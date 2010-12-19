using Raven.Http;

namespace Raven.Database.Server.Responders
{
    public abstract class RequestResponder : AbstractRequestResponder
    {
        public DocumentDatabase Database
        {
            get
            {
                return (DocumentDatabase)ResourceStore;
            }
        }
    }
}