using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Client;

namespace Raven.Client.DynamicQueries
{
    public class DynamicDocumentQuery<T> : DocumentQuery<T>
    {
        public DynamicDocumentQuery(DocumentSession session, IDatabaseCommands commands) 
            : base(session, commands, "dynamic", null)
        {

        }

        protected override Database.Data.QueryResult GetQueryResult()
        {
            var query = this.GenerateIndexQuery(this.QueryText.ToString());
            return this.Commands.DynamicQuery(query, this.Includes.ToArray());           
        }
    }
}
