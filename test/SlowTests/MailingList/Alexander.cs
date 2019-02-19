using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Alexander : RavenTestBase
    {
        [Fact]
        public void QueryById()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { @"
docs.Casinos
    .SelectMany(casino => casino.Comments, (casino, comment) => new {CityId = casino.CityId, CasinoId = Id(casino), Id = comment.Id, DateTime = comment.DateTime, Author = comment.Author, Text = comment.Text})" },

                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        { "CityId", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                        { "CasinoId", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                        { "Id", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                        { "DateTime", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                        { "Author", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                        { "Text", new IndexFieldOptions { Storage = FieldStorage.Yes } },
                    },
                    Name = "CasinosCommentsIndex" }
                }));

                var documentSession = store.OpenSession();

                var casino = new Casino("Cities/123456", "address", "name");
                documentSession.Store(casino);
                documentSession.SaveChanges();

                var casinoFromDb = documentSession.Query<Casino>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Id == casino.Id).Single();
                Assert.NotNull(casinoFromDb);
            }
        }

        private class Casino
        {
            public string Id { get; set; }
            public DateTime AdditionDate { get; set; }
            public string CityId { get; set; }
            public string Address { get; set; }
            public string Title { get; set; }
            public CasinoStatus Status { get; set; }
            public IList<Comment> Comments { get; set; }
            public IList<Suspension> Suspensions { get; set; }

            private Casino()
            {
                Status = CasinoStatus.Opened;
            }

            public Casino(string cityId, string address, string name)
                : this()
            {
                AdditionDate = DateTime.UtcNow;
                CityId = cityId;
                Address = address;
                Title = name;

                Comments = new List<Comment>();
                Suspensions = new List<Suspension>();
            }
        }

        private enum CasinoStatus
        {
            Opened = 1,
            Closed = 2
        }

        private class Comment
        {
            public DateTime DateTime { get; set; }
            public string Author { get; set; }
            public string Text { get; set; }

            public Comment(string author, string text)
            {
                DateTime = DateTime.UtcNow;
                Author = author;
                Text = text;
            }
        }

        private class Suspension
        {
            public DateTime DateTime { get; set; }
            public IList<Exemption> Exemptions { get; set; }

            public Suspension()
            {
                Exemptions = new List<Exemption>();
            }

            public Suspension(DateTime dateTime, IList<Exemption> exemptions)
            {
                DateTime = dateTime;
                Exemptions = exemptions;
            }
        }

        private class Exemption
        {
            public ExemptionItemType ItemType { get; set; }
            public long Quantity { get; set; }

            public Exemption(ExemptionItemType itemType, long quantity)
            {
                ItemType = itemType;
                Quantity = quantity;
            }
        }

        private enum ExemptionItemType
        {
            Unknown = 1,
            Pc = 2,
            SlotMachine = 3,
            Table = 4,
            Terminal = 5
        }
    }
}
