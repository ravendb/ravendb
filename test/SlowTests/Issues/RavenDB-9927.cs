using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9927 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_query_syntax1()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax1());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_query_syntax2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax2());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_query_syntax3()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax3());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_query_syntax4()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax4());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_query_syntax5()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax5());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_method_syntax1()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax1());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_method_syntax2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax2());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_method_syntax3()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax3());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_method_syntax4()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax4());
            }
        }

        [Fact(Skip = "RavenDB-9927")]
        public void Should_not_throw_method_syntax5()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IncidentsByDateQuerySyntax5());
            }
        }

        private class IncidentsByDateQuerySyntax1 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from incident in docs.Incidents
                        select new
                        {
                            Date = incident.OccuredOn,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        Date = result.Date.Date
                    }
                    into g
                    select new
                    {
                        Date = g.Key.Date,
                        Count = g.Sum(x => x.Count)
                    }"
                };
            }
        }

        private class IncidentsByDateQuerySyntax2 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from incident in docs.Incidents
                        select new
                        {
                            Date = incident.OccuredOn,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        Date = result.Date ?? DateTime.Now
                    }
                    into g
                    select new
                    {
                        Date = g.Key.Date,
                        Count = g.Sum(x => x.Count)
                    }"
                };
            }
        }

        private class IncidentsByDateQuerySyntax3 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from incident in docs.Incidents
                        select new
                        {
                            Date = incident.OccuredOn,
                            AnotherDate = incident.OccuredOn,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        CalculatedDate = result.Date ?? result.AnotherDate
                    }
                    into g
                    select new
                    {
                        Date = g.Key.CalculatedDate,
                        AnotherDate = g.Key.CalculatedDate,
                        Count = g.Sum(x => x.Count)
                    }"
                };
            }
        }

        private class IncidentsByDateQuerySyntax4 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from incident in docs.Incidents
                        select new
                        {
                            Date = incident.OccuredOn,
                            AnotherDate = incident.OccuredOn,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        CalculatedDate = ((DateTime?)DateTime.Now) ?? result.AnotherDate
                    }
                    into g
                    select new
                    {
                        Date = g.Key.CalculatedDate,
                        AnotherDate = g.Key.CalculatedDate,
                        Count = g.Sum(x => x.Count)
                    }"
                };
            }
        }

        private class IncidentsByDateQuerySyntax5 : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from incident in docs.Incidents
                        select new
                        {
                            Date = incident.OccuredOn,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    let date = result.Date == null ? DateTime.Now : result.Date == DateTime.MaxValue ? result.Date : DateTime.MinValue
                    group result by new
                    {
                        CalculatedDate = date
                    }
                    into g
                    select new
                    {
                        Date = g.Key.CalculatedDate,
                        Count = g.Sum(x => x.Count)
                    }"
                };
            }
        }

        private class IncidentsByDateMethodSyntax1 : AbstractIndexCreationTask<Incident, IncidentsByDateMethodSyntax1.Result>
        {
            public class Result
            {
                public DateTime Date { get; set; }

                public int Count { get; set; }
            }

            public IncidentsByDateMethodSyntax1()
            {
                Map = incidents => from incident in incidents
                    select new
                    {
                        Date = incident.OccuredOn,
                        Count = 1,
                    };

                Reduce = results => from result in results
                    group result by new
                    {
                        Date = result.Date.Date,
                    }
                    into g
                    select new
                    {
                        Date = g.Key.Date,
                        Count = g.Sum(x => x.Count),
                    };
            }
        }

        private class IncidentsByDateMethodSyntax2 : AbstractIndexCreationTask<Incident, IncidentsByDateMethodSyntax2.Result>
        {
            public class Result
            {
                public DateTime? Date { get; set; }

                public int Count { get; set; }
            }

            public IncidentsByDateMethodSyntax2()
            {
                Map = incidents => from incident in incidents
                    select new
                    {
                        Date = incident.OccuredOn,
                        Count = 1,
                    };

                Reduce = results => from result in results
                    group result by new
                    {
                        Date = result.Date ?? DateTime.Now,
                    }
                    into g
                    select new
                    {
                        Date = g.Key.Date,
                        Count = g.Sum(x => x.Count),
                    };
            }
        }

        private class IncidentsByDateMethodSyntax3 : AbstractIndexCreationTask<Incident, IncidentsByDateMethodSyntax3.Result>
        {
            public class Result
            {
                public DateTime Date { get; set; }

                public int Count { get; set; }
            }

            public IncidentsByDateMethodSyntax3()
            {
                Map = incidents => from incident in incidents
                    select new
                    {
                        Date = incident.OccuredOn,
                        Count = 1,
                    };

                Reduce = results => from result in results
                    group result by new
                    {
                        Date = result.Date.Date,
                    }
                    into g
                    select new
                    {
                        Date = g.Key.Date,
                        Count = g.Sum(x => x.Count),
                    };
            }
        }

        private class IncidentsByDateMethodSyntax4 : AbstractIndexCreationTask<Incident, IncidentsByDateMethodSyntax4.Result>
        {
            public class Result
            {
                public DateTime? Date { get; set; }

                public DateTime AnotherDate { get; set; }

                public int Count { get; set; }
            }

            public IncidentsByDateMethodSyntax4()
            {
                Map = incidents => from incident in incidents
                    select new
                    {
                        Date = incident.OccuredOn,
                        AnotherDate = incident.OccuredOn,
                        Count = 1,
                    };

                Reduce = results => from result in results
                    group result by new
                    {
                        CalculatedDate = ((DateTime?)DateTime.Now) ?? result.AnotherDate
                    }
                    into g
                    select new
                    {
                        Date = g.Key.CalculatedDate,
                        Count = g.Sum(x => x.Count),
                    };
            }
        }

        private class IncidentsByDateMethodSyntax5 : AbstractIndexCreationTask<Incident, IncidentsByDateMethodSyntax5.Result>
        {
            public class Result
            {
                public DateTime Date { get; set; }

                public int Count { get; set; }
            }

            public IncidentsByDateMethodSyntax5()
            {
                Map = incidents => from incident in incidents
                    select new
                    {
                        Date = incident.OccuredOn,
                        Count = 1,
                    };

                Reduce = results => from result in results
                    let date = result.Date == null ? DateTime.Now :
                        result.Date == DateTime.MaxValue ? result.Date : DateTime.MinValue
                    group result by new
                    {
                        CalculatedDate = date,
                    }
                    into g
                    select new
                    {
                        Date = g.Key.CalculatedDate,
                        Count = g.Sum(x => x.Count),
                    };
            }
        }

        public class Incident
        {
            public DateTime OccuredOn { get; set; }
        }
    }
}
