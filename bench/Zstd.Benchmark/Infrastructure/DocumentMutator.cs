using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Produces structurally identical variants of a document so small sample sets can be grown into larger corpora
    /// without introducing byte-identical repeats (which would let the compressor cheat on long streams).
    /// Ids get new numbers, dates are shifted, numbers are perturbed and digit runs (phones, postal codes) are re-rolled.
    /// Strings without digits (names, cities, product names) are kept, the same way real collections repeat their vocabulary.
    /// </summary>
    internal static class DocumentMutator
    {
        private const string DateFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff";

        private static readonly Regex IdPattern = new(@"^(?<prefix>[A-Za-z]+/)(?<number>\d+)(?<suffix>-[A-Z])?$", RegexOptions.Compiled);
        private static readonly Regex DigitRuns = new(@"\d+", RegexOptions.Compiled);

        /// <param name="json">The source document.</param>
        /// <param name="variant">0 returns the document unchanged.</param>
        /// <param name="seed">Must be deterministic (e.g. the document's position) so every benchmark process sees identical data.</param>
        public static string Mutate(string json, int variant, int seed)
        {
            if (variant == 0)
                return json;

            Random random = new(unchecked(variant * 1_000_003 + seed));
            JsonNode root = JsonNode.Parse(json);
            root = Mutate(root, variant, random);
            return root.ToJsonString();
        }

        private static JsonNode Mutate(JsonNode node, int variant, Random random)
        {
            switch (node)
            {
                case null:
                    return null;
                case JsonObject obj:
                    foreach (string key in obj.Select(x => x.Key).ToList())
                    {
                        JsonNode child = obj[key];
                        JsonNode mutated = Mutate(child, variant, random);
                        if (ReferenceEquals(child, mutated) == false)
                            obj[key] = mutated;
                    }
                    return obj;
                case JsonArray array:
                    for (int i = 0; i < array.Count; i++)
                    {
                        JsonNode child = array[i];
                        JsonNode mutated = Mutate(child, variant, random);
                        if (ReferenceEquals(child, mutated) == false)
                            array[i] = mutated;
                    }
                    return array;
                case JsonValue value:
                    return MutateValue(value, variant, random);
                default:
                    return node;
            }
        }

        private static JsonNode MutateValue(JsonValue value, int variant, Random random)
        {
            switch (value.GetValueKind())
            {
                case JsonValueKind.String:
                    return JsonValue.Create(MutateString(value.GetValue<string>(), variant, random));
                case JsonValueKind.Number:
                    if (value.TryGetValue(out long integer))
                    {
                        if (integer == 0)
                            return value;
                        long magnitude = Math.Abs(integer);
                        long mutated = random.NextInt64(1, magnitude * 2 + 1);
                        return JsonValue.Create(integer < 0 ? -mutated : mutated);
                    }

                    double number = value.GetValue<double>();
                    return JsonValue.Create(Math.Round(number * (0.5 + random.NextDouble()), 2));
                default:
                    return value;
            }
        }

        private static string MutateString(string value, int variant, Random random)
        {
            if (value.Length == 0 || value.AsSpan().IndexOfAnyInRange('0', '9') < 0)
                return value;

            Match id = IdPattern.Match(value);
            if (id.Success)
            {
                long number = long.Parse(id.Groups["number"].Value, CultureInfo.InvariantCulture) + variant * 100_000L;
                return id.Groups["prefix"].Value + number.ToString(CultureInfo.InvariantCulture) + id.Groups["suffix"].Value;
            }

            bool utc = value.EndsWith('Z');
            string dateCandidate = utc ? value[..^1] : value;
            if (DateTime.TryParseExact(dateCandidate, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
            {
                DateTime shifted = date.AddDays(random.Next(-2000, 2000)).AddSeconds(random.Next(0, 86400));
                return shifted.ToString(DateFormat, CultureInfo.InvariantCulture) + (utc ? "Z" : string.Empty);
            }

            return DigitRuns.Replace(value, match =>
            {
                StringBuilder sb = new(match.Length);
                for (int i = 0; i < match.Length; i++)
                    sb.Append((char)('0' + (i == 0 && match.Value[0] != '0' ? random.Next(1, 10) : random.Next(0, 10))));
                return sb.ToString();
            });
        }
    }
}
