using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Responders;
using Raven.Http.Abstractions;
using Rhino.Mocks;
using Xunit;

namespace Raven.Client.Tests
{
    class IndexQueryUrl
    {
        [Fact]
        public void can_encode_and_decode_IndexQuery() 
        {
            IndexQuery result = EncodeAndDecodeIndexQuery(new IndexQuery());

            Assert.NotNull(result);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_Start() 
        {
            int expected = Some.Integer();
            var indexQuery = new IndexQuery();

            indexQuery.Start = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(expected, result.Start);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_Query() 
        {
            //  Fails when at least '&' is in the Query, not sure if that is acceptable
            //  Fails because the value has not by url decoded, I couldn't find code doing the url decode
            //  after GetIndexQueryFromHttpContext() so there may be another bug.

            //var expected = new string(Enumerable.Range(0, 255).Select(i => (char)i)
            //    .Where(c => !Char.IsControl(c)).ToArray());

            var expected = Some.String();

            var indexQuery = new IndexQuery();

            indexQuery.Query = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(expected, result.Query);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_Query_pound() 
        {
            var expected = Some.String() + '#' + Some.String();

            var indexQuery = new IndexQuery();

            indexQuery.Query = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(expected, result.Query);
        }

        [Fact(Skip = "Is PageSize always to be reloaded from configuration on the server?  Thats whats happening")]
        public void can_encode_and_decode_IndexQuery_PageSize()
        {
            var expected = Some.Integer();
            var indexQuery = new IndexQuery();

            indexQuery.PageSize = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(expected, result.PageSize);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_FieldsToFetch()
        {
            var firstField = Some.String();
            var secondField = Some.String();
            var indexQuery = new IndexQuery();

            indexQuery.FieldsToFetch = new string[] { firstField, secondField };

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(2, result.FieldsToFetch.Length);
            Assert.Equal(firstField, result.FieldsToFetch[0]);
            Assert.Equal(secondField, result.FieldsToFetch[1]);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_SortedFields()
        {
            SortedField sf1 = new SortedField(Some.String())
            {
                Field = "sf1",
                Descending = true
            };

            SortedField sf2 = new SortedField(Some.String())
            {
                Field = "sf2",
                Descending = false
            };

            SortedField[] expected = new[] { sf1, sf2 };
            var indexQuery = new IndexQuery();

            indexQuery.SortedFields = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(2, result.SortedFields.Length);
            Assert.Equal("sf1", result.SortedFields[0].Field);
            Assert.Equal(true, result.SortedFields[0].Descending);
            Assert.Equal("sf2", result.SortedFields[1].Field);
            Assert.Equal(false, result.SortedFields[1].Descending);
        }

        [Fact]
        public void can_encode_and_decode_IndexQuery_CutOff()
        {
            var expected = DateTime.UtcNow;
            var indexQuery = new IndexQuery();

            indexQuery.Cutoff = expected;

            IndexQuery result = EncodeAndDecodeIndexQuery(indexQuery);

            Assert.Equal(expected, result.Cutoff);
        }

        private static IndexQuery EncodeAndDecodeIndexQuery(IndexQuery query)
        {
            string indexQueryUrl = query.GetIndexQueryUrl(Some.String(), Some.String(), Some.String());

            // indexQueryUrl is in the form "/path?querystring#anchor"

            string indexQueryQuerystring = indexQueryUrl.Substring(indexQueryUrl.IndexOf("?")+1);

            int indexOfPoint = indexQueryQuerystring.IndexOf('#');
            if (indexOfPoint != -1)
            {
                indexQueryQuerystring = indexQueryQuerystring.Substring(0, indexOfPoint);
            }

            IHttpRequest request = MockRepository.GenerateStub<IHttpRequest>();
            IHttpContext context = MockRepository.GenerateMock<IHttpContext>();
            context.Stub(c => c.Request).Return(request);
            request.Stub(r => r.QueryString).Return(HttpUtility.ParseQueryString(indexQueryQuerystring));

            return context.GetIndexQueryFromHttpContext(1024);
        }
    }
}
