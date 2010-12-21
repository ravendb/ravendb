//-----------------------------------------------------------------------
// <copyright file="RequestResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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