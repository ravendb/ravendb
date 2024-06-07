using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers;

public class StreamJsonFileBlittableQueryResultWriter : AbstractStreamJsonFileBlittableQueryResultWriter<BlittableJsonReaderObject>
{
    private bool _first = true;

    public StreamJsonFileBlittableQueryResultWriter([NotNull] HttpResponse response, [NotNull] Stream stream, [NotNull] JsonOperationContext context, string[] properties = null, string jsonFileNamePrefix = "export") 
        : base(response, stream, context, properties, jsonFileNamePrefix)
    {
    }

    public override ValueTask AddResultAsync(BlittableJsonReaderObject res, CancellationToken token)
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

                var propertyIndex = res.GetPropertyIndex(property);
                if (propertyIndex == -1)
                    throw new InvalidOperationException();

                res.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                Writer.WritePropertyName(propertyDetails.Name);
                Writer.WriteValue(propertyDetails.Token & BlittableJsonReaderBase.TypesMask, propertyDetails.Value);
            }

            Writer.WriteEndObject();

            return ValueTask.CompletedTask;
        }

        Writer.WriteObject(res);
        return ValueTask.CompletedTask;
    }
}
