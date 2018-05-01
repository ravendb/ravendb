using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10992 : RavenTestBase
    {
        public enum DocumentStatus
        {
            Default = 0,
            Special = 1
        }

        public class Document
        {
            public string Id { get; set; }
            public List<SubDocument> SubDocuments { get; set; }
        }

        public class SubDocument
        {
            [DefaultValue(DocumentStatus.Default)]
            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public DocumentStatus Status { get; set; }
        }

        public class Result
        {
            public string Id { get; set; }
            public List<SubDocumentResult> SubDocuments { get; set; }
        }

        public class SubDocumentResult
        {
            public DocumentStatus Status { get; set; }
        }

        [Fact]
        public void CanGetDefaultNonSerializedEnumValue()
        {
            DateTimeOffset date = DateTimeOffset.UtcNow.Date;
            using (var store = GetDocumentStore(options: new Options
            {
                ModifyDocumentStore = x => x.Conventions.CustomizeJsonSerializer = c => c.NullValueHandling = NullValueHandling.Ignore
            }))
            {                
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        SubDocuments = new List<SubDocument>
                        {
                            new SubDocument
                            {
                                Status = DocumentStatus.Default
                            }
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var projection =
                        from d in session.Query<Document>()
                            .Customize(x => x.WaitForNonStaleResults())
                        select new Result
                        {
                            Id = d.Id,
                            SubDocuments = d.SubDocuments
                                .Select(x => new SubDocumentResult
                                {
                                    Status = x.Status
                                })
                                .ToList()
                        };

                    var projQuery = projection.ToString();
                    var projectionResult = projection.ToList();

                    var result = Assert.Single(projectionResult);
                    var subDocument = Assert.Single(result.SubDocuments);
                    Assert.Equal(DocumentStatus.Default, subDocument.Status);
                }
            }
        }
    }
}
