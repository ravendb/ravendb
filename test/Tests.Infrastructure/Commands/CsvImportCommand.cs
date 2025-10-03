using System;
using System.IO;
using System.Net.Http;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Tests.Infrastructure.Commands;

public class CsvImportCommand : RavenCommand
{
    private readonly Stream _stream;
    private readonly string _collection;
    private readonly long _operationId;
    private readonly InValidCsvImportOptions _csvConfig;

    public override bool IsReadRequest => false;

    public CsvImportCommand(Stream stream, string collection, long operationId, InValidCsvImportOptions inValidCsvConfiguration = null)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));

        _collection = collection;
        _operationId = operationId;
        _csvConfig = inValidCsvConfiguration;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/smuggler/import/csv?operationId={_operationId}&collection={_collection}";
        var form = new MultipartFormDataContent();

        if (_csvConfig != null)
        {
            var _csvConfigBlittable = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_csvConfig, ctx);
            form = new MultipartFormDataContent
            {
                { new BlittableJsonContent(async stream => { await ctx.WriteAsync(stream, _csvConfigBlittable); }, DocumentConventions.Default), Constants.Smuggler.CsvImportOptions },
                { new StreamContent(_stream), "file", "name" }
            };
        }
        else
        {
            form = new MultipartFormDataContent
            {
                { new StreamContent(_stream), "file", "name" }
            };
        }

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = form
        };
    }
}

public class InValidCsvImportOptions
{
    public string Delimiter { get; set; }
    public string Quote { get; set; } // Quote is char in CSVHelper
    public string Comment { get; set; } // Comment is char in CSVHelper
    public bool AllowComments { get; set; }
    public string TrimOptions { get; set; }
}
