using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers;

public class StreamJsonFileDocumentQueryResultWriter : AbstractStreamJsonFileBlittableQueryResultWriter<Document>
{
    private bool _first = true;

    public StreamJsonFileDocumentQueryResultWriter([NotNull] HttpResponse response, [NotNull] Stream stream, [NotNull] JsonOperationContext context, string[] properties = null, string jsonFileNamePrefix = "export")
        : base(response, stream, context, properties, jsonFileNamePrefix)
    {
    }

    public override ValueTask AddResultAsync(Document res, CancellationToken token)
    {
        if (_first == false)
            Writer.WriteComma();
        else
            _first = false;

        if (Properties != null)
        {
            var innerFirst = true;
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            Writer.WriteStartObject();

            foreach (var property in Properties)
            {
                if (innerFirst == false)
                    Writer.WriteComma();
                else
                    innerFirst = false;

                if (Constants.Documents.Metadata.Id == property)
                {
                    Writer.WritePropertyName(Constants.Documents.Metadata.Id);
                    Writer.WriteString(res.Id.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    var propertyIndex = res.Data.GetPropertyIndex(property);
                    if (propertyIndex == -1)
                        throw new InvalidOperationException();

                    res.Data.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                    Writer.WritePropertyName(propertyDetails.Name);
                    Writer.WriteValue(propertyDetails.Token & BlittableJsonReaderBase.TypesMask, propertyDetails.Value);
                }
            }

            Writer.WriteEndObject();

            return ValueTask.CompletedTask;
        }

        Writer.WriteObject(res.Data);
        return ValueTask.CompletedTask;
    }
}
