using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Indexing;
using System.Text.RegularExpressions;

namespace Raven.Database.Data
{
    public class DynamicQueryMapping
    {
        static readonly Regex QueryTerms = new Regex(@"([^\s\(\+\-][\w._,]+)\:", RegexOptions.Compiled);

        public string ForEntityName { get; set; }

        public DynamicQueryMappingItem[] Items
        {
            get;
            set;
        }

        public IndexDefinition CreateIndexDefinition()
        {
            var fromClauses = new HashSet<string>();
            var realMappings = new List<string>();

            fromClauses.Add("from doc in docs");

            foreach (var map in Items)
            {
                String currentDoc = "doc";
                StringBuilder currentExpression = new StringBuilder();

                int currentIndex = 0;
                while (currentIndex < map.From.Length)
                {
                    char currentChar = map.From[currentIndex++];
                    switch (currentChar)
                    {
                        case ',':                                                       

                            // doc.NewDoc.Items
                            String newDocumentSource =  string.Format("{0}.{1}", currentDoc, currentExpression.ToString());

                            // docNewDocItemsItem
                            String newDoc = string.Format("{0}Item", newDocumentSource.Replace(".", ""));

                            // from docNewDocItemsItem in doc.NewDoc.Items
                            String docInclude = string.Format("from {0} in {1}", newDoc, newDocumentSource);
                            fromClauses.Add(docInclude);

                            // Start building the property again
                            currentExpression.Clear();

                            // And from this new doc
                            currentDoc = newDoc;

                            break;
                        default:
                            currentExpression.Append(currentChar);
                            break;
                    }
                }

                // We get rid of any _Range(s) etc
                realMappings.Add(string.Format("{0} = {1}.{2}",
                        map.To.Replace("_Range", ""),
                        currentDoc,
                        currentExpression.ToString().Replace("_Range", "")
                        ));
            }
            
            return new IndexDefinition()
            {
                 Map = string.Format("{0}\r\nselect new {{ {1} }}",
                    string.Join("\r\n", fromClauses.ToArray()),
                    string.Join(", ", realMappings.ToArray())),
            };
        }

        public static DynamicQueryMapping Create(string query, string entityName)
        {
            var queryTermMatches = QueryTerms.Matches(query);
             var fields = new HashSet<string>();
            for (int x = 0; x < queryTermMatches.Count; x++)
            {
                Match match = queryTermMatches[x];
                String field = match.Groups[1].Value;
                fields.Add(field);
            }
            
            return new DynamicQueryMapping()
            {
                ForEntityName = entityName,
                Items = fields.Select(x => new DynamicQueryMappingItem()
                {
                    From = x,
                    To = x.Replace(".", "").Replace(",", "")
                }).ToArray()
            };
                      
        }


        
    }
}
