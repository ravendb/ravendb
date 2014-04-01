using System;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class ActualValueInJsonReaderException : RavenTest
    {
        [Fact]
        public async Task JsonErrorsShouldIncludeOriginalData()
        {
            var responseMesage = new HttpResponseMessage
            {
                ReasonPhrase = "<>,./:",
                Content = new StringContent("<>,./:")
            };

            var responseException = ErrorResponseException.FromResponseMessage(responseMesage, true);
            var exception = await AssertAsync.Throws<InvalidOperationException>(async () => await responseException.TryReadErrorResponseObject<string>());
            Assert.Contains("Exception occured reading the string: ", exception.Message);
        }
    }
}