using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10876 : RavenTestBase
    {        
        [Theory]        
        [InlineData(32)]
        [InlineData(100)]
        [InlineData(308)]              
        public void ShouldSupportRawNumbersIntegers(int stringLength)
        {
            string GetNumber(int length)
            {
                var sb = new StringBuilder();
                for (var i=1; i<= length; i++)
                {
                    sb.Append(((char)((byte)'0' + (i % 10))).ToString());
                }
                return sb.ToString();
            }
            string bigNumString = string.Join(string.Empty, GetNumber(stringLength));

            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {                    
                    var reader = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["BigNumber"] = new LazyNumberValue(context.GetLazyString(bigNumString))
                    }, "num");
                    requestExecuter.Execute(new PutDocumentCommand("bignum/1", null, reader), context);
                }

                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {

                    GetDocumentsCommand getDoc = new GetDocumentsCommand("bignum/1", null, false);
                    requestExecuter.Execute(getDoc, context);
                    var doc = getDoc.Result.Results[0] as BlittableJsonReaderObject;
                    Assert.True(doc.TryGet<LazyNumberValue>("BigNumber", out var rawNum));
                    Assert.Equal(bigNumString, rawNum.Inner.ToString());
                }
            }
        }

        [Theory]
        [InlineData(2)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(100)]
        [InlineData(308)]
        public void ShouldSupportRawNumbersFloatingPoint(int stringLength)
        {
            string GetNumber(int length)
            {
                var sb = new StringBuilder("0.");
                for (var i = 1; i <= length; i++)
                {
                    sb.Append(((char)((byte)'0' + (i % 10))).ToString());
                }
                return sb.ToString();
            }
            string bigNumString = string.Join(string.Empty, GetNumber(stringLength));

            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["BigNumber"] = new LazyNumberValue(context.GetLazyString(bigNumString))
                    }, "num");
                    requestExecuter.Execute(new PutDocumentCommand("bignum/1", null, reader), context);
                }

                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {

                    GetDocumentsCommand getDoc = new GetDocumentsCommand("bignum/1", null, false);
                    requestExecuter.Execute(getDoc, context);
                    var doc = getDoc.Result.Results[0] as BlittableJsonReaderObject;
                    Assert.True(doc.TryGet<LazyNumberValue>("BigNumber", out var rawNum));
                    Assert.Equal(bigNumString, rawNum.Inner.ToString());
                }
            }
        }

        [Fact]
        public void ShouldNotSupportRawNumbersBiggerThenDoubleMaxVal()
        {            
            string bigNumString = string.Join(string.Empty, "17976931348623158" + string.Join("", Enumerable.Repeat(0,293)));

            using (var store = GetDocumentStore())
            {
                var requestExecuter = store.GetRequestExecutor();
                using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
                {
                    var reader = context.ReadObject(new Sparrow.Json.Parsing.DynamicJsonValue
                    {
                        ["BigNumber"] = new LazyNumberValue(context.GetLazyString(bigNumString))
                    }, "num");
                    var thrownException =  Assert.Throws<RavenException>(() => requestExecuter.Execute(new PutDocumentCommand("bignum/1", null, reader), context));

                    Assert.StartsWith("System.IO.InvalidDataException: Could not parse double:", thrownException.Message);
                }              
            }
        }        
    }
    
}
