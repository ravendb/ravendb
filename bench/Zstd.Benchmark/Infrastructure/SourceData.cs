using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Raw JSON documents taken from real-life sample data shipped in the repository.
    /// </summary>
    internal static class SourceData
    {
        private static readonly Lazy<byte[]> NorthwindDumpBytes = new(() =>
        {
            using FileStream file = File.OpenRead(DataPath("Northwind.ravendbdump"));
            using GZipStream gzip = new(file, CompressionMode.Decompress);
            using MemoryStream ms = new();
            gzip.CopyTo(ms);
            return ms.ToArray();
        });

        private static readonly Lazy<Dictionary<string, List<string>>> NorthwindCollections = new(LoadNorthwindCollections);

        private static readonly Lazy<List<string>> NorthwindDocumentsInDumpOrder = new(() => ReadDumpDocuments(NorthwindDumpBytes.Value).Select(x => x.Json).ToList());

        private static readonly Lazy<List<string>> Monsters = new(LoadMonsters);

        /// <summary>
        /// The decompressed Northwind smuggler dump: documents, revisions and binary attachments, exactly as an export produces it.
        /// </summary>
        public static byte[] NorthwindDump => NorthwindDumpBytes.Value;

        public static IReadOnlyList<string> NorthwindDocuments => NorthwindDocumentsInDumpOrder.Value;

        public static IReadOnlyList<string> GetDocuments(string dataset)
        {
            switch (dataset)
            {
                case "Orders":
                case "Companies":
                case "Products":
                case "Employees":
                    return NorthwindCollections.Value[dataset];
                case "CompanyWithOrders":
                    return BuildCompanyWithOrders();
                case "Monsters":
                    return Monsters.Value;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dataset), dataset, "Unknown dataset");
            }
        }

        public static string DataPath(string fileName) => Path.Combine(AppContext.BaseDirectory, "Data", fileName);

        private static Dictionary<string, List<string>> LoadNorthwindCollections()
        {
            Dictionary<string, List<string>> collections = new(StringComparer.OrdinalIgnoreCase);
            foreach ((string collection, string json) in ReadDumpDocuments(NorthwindDumpBytes.Value))
            {
                if (collections.TryGetValue(collection, out List<string> list) == false)
                    collections[collection] = list = new List<string>();
                list.Add(json);
            }

            return collections;
        }

        /// <summary>
        /// Reads the "Docs" array of a smuggler dump. Attachments are written as a JSON header followed by raw bytes, those are skipped.
        /// </summary>
        private static IEnumerable<(string Collection, string Json)> ReadDumpDocuments(byte[] dump)
        {
            byte[] marker = Encoding.UTF8.GetBytes("\"Docs\":[");
            int position = dump.AsSpan().IndexOf(marker);
            if (position < 0)
                throw new InvalidDataException("No 'Docs' array in the dump");
            position += marker.Length;

            List<(string, string)> results = new();
            while (true)
            {
                while (char.IsWhiteSpace((char)dump[position]) || dump[position] == (byte)',')
                    position++;

                if (dump[position] == (byte)']')
                    break;

                Utf8JsonReader reader = new(dump.AsSpan(position), isFinalBlock: false, state: default);
                using JsonDocument document = JsonDocument.ParseValue(ref reader);
                position += (int)reader.BytesConsumed;

                JsonElement metadata = document.RootElement.GetProperty("@metadata");
                if (metadata.TryGetProperty("@export-type", out JsonElement exportType) && exportType.GetString() == "Attachment")
                {
                    position += checked((int)document.RootElement.GetProperty("Size").GetInt64());
                    continue;
                }

                results.Add((metadata.GetProperty("@collection").GetString(), document.RootElement.GetRawText()));
            }

            return results;
        }

        private static List<string> LoadMonsters()
        {
            using JsonDocument document = JsonDocument.Parse(File.ReadAllBytes(DataPath("monsters.json")));
            return document.RootElement.GetProperty("array").EnumerateArray().Select(x => x.GetRawText()).ToList();
        }

        /// <summary>
        /// Aggregate-style documents (a company with its orders embedded) - larger documents with repeating inner structure.
        /// </summary>
        private static List<string> BuildCompanyWithOrders()
        {
            Dictionary<string, List<JsonNode>> ordersByCompany = new(StringComparer.OrdinalIgnoreCase);
            foreach (string orderJson in NorthwindCollections.Value["Orders"])
            {
                JsonObject order = JsonNode.Parse(orderJson).AsObject();
                order.Remove("@metadata");
                string company = order["Company"].GetValue<string>();
                if (ordersByCompany.TryGetValue(company, out List<JsonNode> list) == false)
                    ordersByCompany[company] = list = new List<JsonNode>();
                list.Add(order);
            }

            List<string> results = new();
            foreach (string companyJson in NorthwindCollections.Value["Companies"])
            {
                JsonObject company = JsonNode.Parse(companyJson).AsObject();
                string id = company["@metadata"]["@id"].GetValue<string>();
                if (ordersByCompany.TryGetValue(id, out List<JsonNode> orders) == false)
                    continue;

                company["Orders"] = new JsonArray(orders.ToArray());
                results.Add(company.ToJsonString());
            }

            return results;
        }
    }
}
