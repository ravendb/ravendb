using System.Collections.Generic;

using Raven.Client.Data;

namespace Raven.Server.Documents.Queries
{
    public class MoreLikeThisRequest : MoreLikeThisQuery
    {
        public new Dictionary<string, object> TransformerParameters { get; set; }
    }
}