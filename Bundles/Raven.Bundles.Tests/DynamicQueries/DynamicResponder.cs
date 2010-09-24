using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using System.Net;
using Raven.Client.Client;

namespace Raven.Bundles.Tests.DynamicQueries
{
    public class DynamicResponder : DynamicQueriesBase
    {
        [Fact]
        public void WhenDynamicUrlIsInvoked_QueryResultsAreReturned()
        {
            var request = HttpJsonRequest.CreateHttpJsonRequest(
                this, 
                store.Url + "dynamicquery?query=" + string.Format("name=hello") , "GET", 
                store.Credentials);

               var response = request.ReadResponseString();
           
        }
    }
}
