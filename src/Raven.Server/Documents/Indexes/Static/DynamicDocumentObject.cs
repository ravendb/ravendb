using System.Dynamic;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicDocumentObject : DynamicObject
    {
        private readonly Document _document;
        private readonly DynamicBlittableJson _dynamicJson;

        public DynamicDocumentObject(Document document)
        {
            _document = document;
            _dynamicJson = new DynamicBlittableJson(document.Data);
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            var name = binder.Name;

            if (name == Constants.DocumentIdFieldName)
            {
                result = (string)_document.Key;
                return true;
            }

            if (name == "Id")
            {
                result = (string)_document.Key;
                return true;
            }

            var getResult = _dynamicJson.TryGetMember(binder, out result);

            if (name == "HasValue" && result == null)
            {
                result = false;
                return true;
            }

            if (result is LazyDoubleValue)
            {
                double doubleResult;
                long longResult;

                switch (BlittableNumber.Parse(result, out doubleResult, out longResult))
                {
                    case NumberParseResult.Double:
                        result = doubleResult;
                        break;
                    case NumberParseResult.Long:
                        result = longResult;
                        break;
                }
            }

            //if (result is LazyStringValue)
            //{
            //    TODO arek - this is necessary only to handle methods of string - e.g. .Substring
            //    we should be able to recognize that base on definition of static index
            //    result = result.ToString(); 
            //}

            return getResult;
        }
    }
}