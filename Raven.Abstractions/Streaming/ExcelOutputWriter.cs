using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Streaming
{
    public class ExcelOutputWriter : IOutputWriter
    {
        private const string CsvContentType = "text/csv";

        private readonly Stream stream;
        private readonly string[] customColumns;
        private StreamWriter writer;
        private bool doIncludeId;

        public ExcelOutputWriter(Stream stream, string[] customColumns = null)
        {
            this.stream = stream;
            this.customColumns = customColumns;
        }

        public string ContentType => CsvContentType;

        public void Dispose()
        {
            if (writer == null)
                return;

            writer.Flush();
            stream.Flush();
#if !DNXCORE50
            writer.Close();
#else
            writer.Dispose();
#endif
        }

        public void WriteHeader()
        {
            writer = new StreamWriter(stream, Encoding.UTF8);
        }

        public void Write(RavenJObject result)
        {
            if (properties == null)
            {
                GetPropertiesAndWriteCsvHeader(result, out doIncludeId);
                Debug.Assert(properties != null);
            }

            if (doIncludeId)
            {
                RavenJToken token;
                if (result.TryGetValue("@metadata", out token))
                {
                    var metadata = token as RavenJObject;
                    if (metadata != null)
                    {
                        if (metadata.TryGetValue("@id", out token))
                        {
                            OutputCsvValue(token.Value<string>());
                        }
                        writer.Write(',');
                    }
                }
            }

            foreach (var property in properties)
            {
                var token = result.SelectToken(property);
                if (token != null)
                {
                    switch (token.Type)
                    {
                        case JTokenType.Null:
                            break;

                        case JTokenType.Array:
                        case JTokenType.Object:
                            OutputCsvValue(token.ToString(Formatting.None));
                            break;

                        default:
                            OutputCsvValue(token.Value<string>());
                            break;
                    }
                }

                writer.Write(',');
            }

            writer.WriteLine();
        }

        public void Write(string result)
        {
            OutputCsvValue(result);
            writer.WriteLine();
        }

        public void WriteError(Exception exception)
        {
            writer.WriteLine();
            writer.WriteLine();
            writer.WriteLine(exception.ToString());
        }

        public void Flush()
        {
            writer.Flush();
        }

        private void GetPropertiesAndWriteCsvHeader(RavenJObject result, out bool includeId)
        {
            includeId = false;
            properties = DocumentHelpers.GetPropertiesFromJObject(result,
                parentPropertyPath: "",
                includeNestedProperties: true,
                includeMetadata: false,
                excludeParentPropertyNames: true).ToList();

            if (customColumns != null && customColumns.Length > 0)
            {
                // since user defined custom CSV columns filter list generated using GetPropertiesFromJObject
                // we interate over customColumns instead of properties to maintain columns order requested by user
                properties = customColumns
                    .SelectMany(c => properties.Where(p => p.StartsWith(c)))
                    .ToList();
            }

            RavenJToken token;
            if (result.TryGetValue("@metadata", out token))
            {
                var metadata = token as RavenJObject;
                if (metadata != null)
                {
                    if (metadata.TryGetValue("@id", out token))
                    {
                        OutputCsvValue("@id");
                        writer.Write(',');

                        includeId = true;
                    }
                }
            }

            foreach (var property in properties)
            {
                OutputCsvValue(property);
                writer.Write(',');
            }
            writer.WriteLine();
        }

        private static readonly char[] RequireQuotesChars = { ',', '\r', '\n', '"' };
        private IEnumerable<string> properties;

        private void OutputCsvValue(string val)
        {
            var needsQuoutes = val.IndexOfAny(RequireQuotesChars) != -1;
            if (needsQuoutes)
                writer.Write('"');

            writer.Write(needsQuoutes ? val.Replace("\"", "\"\"") : val);
            if (needsQuoutes)
                writer.Write('"');
        }

    }
}