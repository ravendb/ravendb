using System.Linq;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Indexes;

namespace Raven.Bundles.Tests.Authorization.Bugs.Wayne
{
    public class MessagesForAccountByCreatorId : AbstractTransformerCreationTask<WallMessage<AuthorizationUser>>
    {
        public MessagesForAccountByCreatorId()
        {
            TransformResults = docs => from doc in docs
                let creator = LoadDocument<AuthorizationUser>(doc.Creator.Id)
                let recipient = LoadDocument<AuthorizationUser>(doc.Recipient.Id)
                select new
                {
                    Creator = creator,
                    Recipient = recipient,
                    doc.Id,
                    doc.MessageBody
                };
        }
    }
}