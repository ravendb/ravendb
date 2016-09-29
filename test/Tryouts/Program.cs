using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Linq;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Sparrow;
using Voron;

namespace Tryouts
{
    public class Program
    {
        private static readonly ByteStringContext _byteStringContext = new ByteStringContext();

        public static void Main(string[] args)
        {

            using (var a = new FastTests.Server.Documents.Indexing.Static.BasicStaticMapIndexing())
            {
                a.NumberOfDocumentsAndTombstonesToProcessShouldBeCalculatedCorrectly();
            }
            //var start = Slices.BeforeAllKeys;

            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath("so")))
            //{
            //    int i = 0;
            //    bool hasMore = true; ; 
            //    while (hasMore)
            //    {
            //        i++;
            //        using (var output = File.Create($"posts-{i:000}.dump"))
            //        using (var gzip = new GZipStream(output, CompressionMode.Compress))
            //        using (var writer = new StreamWriter(gzip))
            //        using (var jsonWriter = new JsonTextWriter(writer))
            //        {
            //            Console.WriteLine("\r"+output.Name);
            //            jsonWriter.WriteStartObject();
            //            jsonWriter.WritePropertyName("BuildVersion");
            //            jsonWriter.WriteValue(40000);
            //            jsonWriter.WritePropertyName("Docs");
            //            jsonWriter.WriteStartArray();
            //            hasMore = WriteInOneTransaction(env, ref start, jsonWriter, output);
            //            jsonWriter.WriteEndArray();
            //            jsonWriter.WriteEndObject();
            //        }
            //    }
            //}
        }

        private static bool WriteInOneTransaction(StorageEnvironment env, ref Slice start, JsonTextWriter jsonWriter, Stream output)
        {
            using (var tx = env.ReadTransaction())
            {
                var tree = tx.ReadTree("q");
                using (var it = tree.Iterate(prefetch: true))
                {
                    if (it.Seek(start) == false)
                        return false;

                    XmlDocument xmlDoc = new XmlDocument();
                    int count = 0;
                    var parts = it.CurrentKey.ToString().Split(',');
                    var qId = parts[0];
                    RavenJObject question = null;
                    ;
                    var answers = new List<RavenJObject>();
                    do
                    {
                        parts = it.CurrentKey.ToString().Split(',');

                        if (qId != parts[0])
                        {
                            foreach (var answer in answers)
                            {
                                question.Value<RavenJArray>("Answers").Add(answer);
                            }
                            question.WriteTo(jsonWriter);
                            answers.Clear();
                            question = null;
                            qId = parts[0];
                        }
                        var xmlReader = XmlReader.Create(new StringReader(it.CreateReaderForCurrent().ToStringValue()));
                        var node = xmlDoc.ReadNode(xmlReader);
                        if (parts[1] == qId)
                        {
                            //found the question
                            question = new RavenJObject();

                            AddProperties(question, node);
                            question.Remove("Id");

                            question["Answers"] = new RavenJArray();
                            question["@metadata"] = new RavenJObject
                            {
                                ["Raven-Entity-Name"] = "Questions",
                                ["@id"] = "questions/" + parts[0]
                            };
                        }
                        else
                        {
                            var answer = new RavenJObject();
                            AddProperties(answer, node);
                            answers.Add(answer);
                        }
                        if (count++%5000 == 0)
                        {
                            Console.Write($"\r{count:#,#} processed");
                        }
                        if (output.Length > 1024*1024*256)
                        {
                            if (it.MoveNext() == false)
                                return false;
                            Slice.From(_byteStringContext, qId, out start);
                            return true;
                        }
                    } while (it.MoveNext());
                }
            }
            return false;
        }

        private static void AddProperties(RavenJObject question, XmlNode node)
        {
            foreach (XmlAttribute attribute in node.Attributes)
            {

                switch (attribute.LocalName)
                {
                    case "PostTypeId":
                    case "ParentId":
                        break;
                    case "Tags":
                        question["Tags"] =
                            new RavenJArray(attribute.Value.Trim('<', '>')
                                .Split(new[] {"><"}, StringSplitOptions.RemoveEmptyEntries));
                        break;
                    default:
                        long val;
                        if (long.TryParse(attribute.Value, out val))
                            question[attribute.LocalName] = val;
                        else
                            question[attribute.LocalName] = attribute.Value;
                        break;
                }
            }
        }
    }

    public class Question
    {
        public long AcceptedAnswerId { get; set; }
        public DateTime CreationDate { get; set; }
        public int Score { get; set; }
        public int ViewCount { get; set; }
        public string Body { get; set; }
        public string OwnerUserId { get; set; }
        public string LastEditorUserId { get; set; }
        public string LastEditorDisplayName { get; set; }
        public DateTime LastEditDate { get; set; }
        public DateTime LastActivityDate { get; set; }
        public string Title { get; set; }
        public string[] Tags { get; set; }

        public int AnswrCount { get; set; }
        public int CommentCount { get; set; }
        public int FavoriteCount { get; set; }

        public List<Answer> Answers { get; set; } = new List<Answer>();
    }

    public class Answer
    {
        public DateTime CreationDate { get; set; }
        public int Score { get; set; }
        public string OwnerUserId { get; set; }
        public string LastEditorUserId { get; set; }
        public string LastEditorDisplayName { get; set; }
        public DateTime LastEditDate { get; set; }
        public DateTime LastActivityDate { get; set; }
        public DateTime CommunityOwnedDate { get; set; }
        public int CommentCount { get; set; }
    }
}
