using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21263 : ReplicationTestBase
    {
        public RavenDB_21263(ITestOutputHelper output) : base(output)
        {
        }

        private const string OldJsonRecord = "{\r\n    \"BuildVersion\": 54,\r\n    \"DatabaseRecord\": {\r\n        \"DatabaseName\": \"test111\",\r\n        \"Encrypted\": false,\r\n        \"UnusedDatabaseIds\": [],\r\n        \"LockMode\": \"Unlock\",\r\n        \"ConflictSolverConfig\": null,\r\n        \"Settings\": [],\r\n       \"Revisions\": {\r\n            \"Default\": null,\r\n            \"Collections\": {\r\n                \"Orders\": {\r\n                    \"Disabled\": false,\r\n                    \"MinimumRevisionsToKeep\": null,\r\n                    \"MinimumRevisionAgeToKeep\": null,\r\n                    \"PurgeOnDelete\": false,\r\n                    \"MaximumRevisionsToDeleteUponDocumentUpdate\": null\r\n                }\r\n            }\r\n        },\r\n        \"TimeSeries\": {},\r\n        \"DocumentsCompression\": {\r\n            \"Collections\": [],\r\n            \"CompressAllCollections\": false,\r\n            \"CompressRevisions\": true\r\n        },\r\n        \"Expiration\": null,\r\n        \"Refresh\": null,\r\n        \"Client\": null,\r\n        \"Sorters\": {},\r\n        \"Analyzers\": {},\r\n          \"RavenConnectionStrings\": {},\r\n        \"SqlConnectionStrings\": {},\r\n        \"PeriodicBackups\": [],\r\n        \"ExternalReplications\": [],\r\n        \"RavenEtls\": [],\r\n        \"SqlEtls\": [],\r\n        \"HubPullReplications\": [],\r\n        \"SinkPullReplications\": [],\r\n        \"OlapConnectionStrings\": {},\r\n        \"OlapEtls\": [],\r\n        \"ElasticSearchConnectionStrings\": {},\r\n        \"ElasticSearchEtls\": [],\r\n        \"QueueConnectionStrings\": {},\r\n        \"QueueEtls\": []\r\n    },\r\n    \"Docs\": [],\r\n    \"RevisionDocuments\": [{\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-OSKWIRBEDEGoAxbEIiFJeQ\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-F9I6Egqwm0Kz+K0oFVIR9Q\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2144-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.8295488Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"City\": \"Charleroi\",\r\n                \"Region\": null,\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Country\": \"Belgium\",\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                }\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"Freight\": 51.3000,\r\n            \"Lines\": [],\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:3804-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.9801503Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8000,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5478-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1021446Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0000,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5480-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1022519Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"C:10323059-46A/cZk8/UuwYdG5Dji0Sg, B:455420525-C3U1veikQEKrM2RUNtZQ5A, B:641180022-DxzjyxXS1EqgSEO3J5hgpw, B:4797716-K8YhRiUCo0GzECyk/JJeKw, A:643125941-MJQwORy0mkes5E53+IAkrg, A:11784715-Mgugu/MVqEOgJNafGyKIfg, B:3985724-QRjh5tTLokWSvDJDbF6JeQ, C:15033473-S1HWITSRQkSzFIy40y9ayg, A:8031522-UjG7submxUSZV+UH3Dx3+g, C:456275656-VszrCLgjjE+jfH1HCyLRSA, B:15060618-ekC7E4AuOU2O8fgRey3iJA, B:10321238-ez7yDfG04k+VpltCkY5frw, A:15575366-kd+eerHmN0ulK2L+gCPIeA, C:8198351-l1IVYfwRNUWjea1mFGMYhg, A:447847892-pX0Cwmvv50WIRxf45m2MtQ, C:639094708-sb+TC4f3XEGcC7vnkjCTrA, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw, A:29931-+oKB0eox0kKFRmpZhHdpYA, A:58113-48XOT93s0keG852a0alBCA, A:80057-8MXXSiK/j02SPt6WvMBp3g, A:34139-J0psmFwcEk+f2D6rB5uSKg, A:53822-RCHmPvRlA0qCqyNWNH7Ufg, A:51048-RL5qTmLhV0SwOwKPeOgf4A, A:105702-b8TkJER0y0mlX3cdUnPJ7A, A:86566-hJl/w96QZ0S1Nrnj73I8kA, A:272221-lF+69deD8UCyPIClNBHGzA, A:53182-p3oH6z9j/0WoL33F0GT6dw, A:65536-yAs2j2d7EUKMYy7+D+oBEw, B:67152-BgN7ypHCN0eZLbWQW4pSbA, B:29926-uAcwGmeZFECexya2bfY6MA, B:54928-z0A6qAIgbkeiDwvkcMmsLQ, C:65537-3N4Tf0UvQUSG+Zg5q8f41A, C:58113-HdKAr6tiA0OhTCazgYuK8A, C:54931-agGw7kyp/EmrroDbznOceQ, C:53822-fa7cPVbGKEWWXcPFlgCy0g, C:50774-lZTIFvUSQEiC7pboTgiYxg, C:51048-qnNDTHlOPUWL/4484fXkz, C:53182-ts+A2CssQkiaYR3vSyIrlQ, D:54929-/15CzUoPak+RM2pwW2arnw, D:54929-/15CzUoPak+RM2pwW4arnw, B:54929-/15CzUoPak+RM2pwW2arnO, B:54955-/15C4joPak+RM2pwW2arnO, B:64929-/1589UoPak+RM2pwW2arnO, A:74929-/7jCzUoPak+RM2pwW2arnO, C:43929-/15CzUoPiu+RM2pwW2arnO\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1024494Z\"\r\n            }\r\n        }, {\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw\",\r\n                \"@flags\": \"HasRevisions, DeleteRevision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2024-01-18T12:12:36.5474797Z\"\r\n            }\r\n        }\r\n    ]\r\n}\r\n";

        private const string NewJsonRecord = "{\r\n    \"BuildVersion\": 54,\r\n    \"DatabaseRecord\": {\r\n        \"DatabaseName\": \"test111\",\r\n        \"Encrypted\": false,\r\n    \"SupportedFeatures\": [\"ThrowRevisionKeyTooBigFix\"],\r\n    \"UnusedDatabaseIds\": [],\r\n        \"LockMode\": \"Unlock\",\r\n        \"ConflictSolverConfig\": null,\r\n        \"Settings\": [],\r\n       \"Revisions\": {\r\n            \"Default\": null,\r\n            \"Collections\": {\r\n                \"Orders\": {\r\n                    \"Disabled\": false,\r\n                    \"MinimumRevisionsToKeep\": null,\r\n                    \"MinimumRevisionAgeToKeep\": null,\r\n                    \"PurgeOnDelete\": false,\r\n                    \"MaximumRevisionsToDeleteUponDocumentUpdate\": null\r\n                }\r\n            }\r\n        },\r\n        \"TimeSeries\": {},\r\n        \"DocumentsCompression\": {\r\n            \"Collections\": [],\r\n            \"CompressAllCollections\": false,\r\n            \"CompressRevisions\": true\r\n        },\r\n        \"Expiration\": null,\r\n        \"Refresh\": null,\r\n        \"Client\": null,\r\n        \"Sorters\": {},\r\n        \"Analyzers\": {},\r\n          \"RavenConnectionStrings\": {},\r\n        \"SqlConnectionStrings\": {},\r\n        \"PeriodicBackups\": [],\r\n        \"ExternalReplications\": [],\r\n        \"RavenEtls\": [],\r\n        \"SqlEtls\": [],\r\n        \"HubPullReplications\": [],\r\n        \"SinkPullReplications\": [],\r\n        \"OlapConnectionStrings\": {},\r\n        \"OlapEtls\": [],\r\n        \"ElasticSearchConnectionStrings\": {},\r\n        \"ElasticSearchEtls\": [],\r\n        \"QueueConnectionStrings\": {},\r\n        \"QueueEtls\": []\r\n    },\r\n    \"Docs\": [],\r\n    \"RevisionDocuments\": [{\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-OSKWIRBEDEGoAxbEIiFJeQ\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-F9I6Egqwm0Kz+K0oFVIR9Q\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2144-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.8295488Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"City\": \"Charleroi\",\r\n                \"Region\": null,\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Country\": \"Belgium\",\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                }\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"Freight\": 51.3000,\r\n            \"Lines\": [],\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:3804-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.9801503Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8000,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5478-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1021446Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0000,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5480-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1022519Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"C:10323059-46A/cZk8/UuwYdG5Dji0Sg, B:455420525-C3U1veikQEKrM2RUNtZQ5A, B:641180022-DxzjyxXS1EqgSEO3J5hgpw, B:4797716-K8YhRiUCo0GzECyk/JJeKw, A:643125941-MJQwORy0mkes5E53+IAkrg, A:11784715-Mgugu/MVqEOgJNafGyKIfg, B:3985724-QRjh5tTLokWSvDJDbF6JeQ, C:15033473-S1HWITSRQkSzFIy40y9ayg, A:8031522-UjG7submxUSZV+UH3Dx3+g, C:456275656-VszrCLgjjE+jfH1HCyLRSA, B:15060618-ekC7E4AuOU2O8fgRey3iJA, B:10321238-ez7yDfG04k+VpltCkY5frw, A:15575366-kd+eerHmN0ulK2L+gCPIeA, C:8198351-l1IVYfwRNUWjea1mFGMYhg, A:447847892-pX0Cwmvv50WIRxf45m2MtQ, C:639094708-sb+TC4f3XEGcC7vnkjCTrA, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw, A:29931-+oKB0eox0kKFRmpZhHdpYA, A:58113-48XOT93s0keG852a0alBCA, A:80057-8MXXSiK/j02SPt6WvMBp3g, A:34139-J0psmFwcEk+f2D6rB5uSKg, A:53822-RCHmPvRlA0qCqyNWNH7Ufg, A:51048-RL5qTmLhV0SwOwKPeOgf4A, A:105702-b8TkJER0y0mlX3cdUnPJ7A, A:86566-hJl/w96QZ0S1Nrnj73I8kA, A:272221-lF+69deD8UCyPIClNBHGzA, A:53182-p3oH6z9j/0WoL33F0GT6dw, A:65536-yAs2j2d7EUKMYy7+D+oBEw, B:67152-BgN7ypHCN0eZLbWQW4pSbA, B:29926-uAcwGmeZFECexya2bfY6MA, B:54928-z0A6qAIgbkeiDwvkcMmsLQ, C:65537-3N4Tf0UvQUSG+Zg5q8f41A, C:58113-HdKAr6tiA0OhTCazgYuK8A, C:54931-agGw7kyp/EmrroDbznOceQ, C:53822-fa7cPVbGKEWWXcPFlgCy0g, C:50774-lZTIFvUSQEiC7pboTgiYxg, C:51048-qnNDTHlOPUWL/4484fXkz, C:53182-ts+A2CssQkiaYR3vSyIrlQ, D:54929-/15CzUoPak+RM2pwW2arnw, D:54929-/15CzUoPak+RM2pwW4arnw, B:54929-/15CzUoPak+RM2pwW2arnO, B:54955-/15C4joPak+RM2pwW2arnO, B:64929-/1589UoPak+RM2pwW2arnO, A:74929-/7jCzUoPak+RM2pwW2arnO, C:43929-/15CzUoPiu+RM2pwW2arnO\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1024494Z\"\r\n            }\r\n        }, {\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw\",\r\n                \"@flags\": \"HasRevisions, DeleteRevision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2024-01-18T12:12:36.5474797Z\"\r\n            }\r\n        }\r\n    ]\r\n}\r\n";

        private const string ExceptionMessage = "Revision change vector cannot exceed 1536 bytes";

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        [RavenData(true)]
        [RavenData(false)]
        public async Task ImportRevisionsWithLargeChangeVectorShouldThrow(Options options, bool useOldVersion)
        {
            // Import always treats the database as a new database, so regardless of the actual version, 
            // it should throw an exception if a revision has a change vector larger than 512 bytes.

            using (var store = GetDocumentStore(options))
            {
                var jsonString = useOldVersion ? OldJsonRecord : NewJsonRecord;
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
                await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    zipStream.Flush();
                    ms.Position = 0;

                    var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        var operation = await store.Smuggler.ForDatabase(store.Database)
                            .ImportAsync(new DatabaseSmugglerImportOptions(), ms);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });

                    Assert.Contains(ExceptionMessage, exception.Message);
                }

                await AssertSupportedFeaturesAsync(store, oldVersion: false);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        [RavenData(true)]
        [RavenData(false)]
        public async Task ImportFromOlderVersionShouldAddSupportedFeatures(Options options, bool supportedFeaturesExists)
        {
            // Import always treats the database as a new database, so regardless of the version exists in the 'RAVENDBDUMPFILE', 
            // the database record should contain "SupportedFeatures: ["ThrowRevisionKeyTooBig"]"

            using (var store = GetDocumentStore(options))
            {
                var name = supportedFeaturesExists ? "test1111-emptySupportedFeatures.ravendbdump" : "test1111-withoutSupportedFeatures.ravendbdump";
                await using (var stream = GetDump("RavenDB_21263." + name))
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                await AssertSupportedFeaturesAsync(store, oldVersion: false);
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        public async Task RestoreFromBackupWithoutDatabaseVersionShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                // convert the JSON database record into a memory stream
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(OldJsonRecord)))
                await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    zipStream.Flush();
                    ms.Position = 0;

                    // create a backup path and write the compressed stream to a file
                    var backupPath = NewDataPath(suffix: "BackupFolder", forceCreateDir: true);
                    var filePath = Path.Combine(backupPath, "test111.ravendb-full-backup");
                    await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                    {
                        await ms.CopyToAsync(fileStream);
                    }

                    var databaseName = "test111";
                    var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName };

                    var restoreOperation = new RestoreBackupOperation(restoreConfig);
                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    using (var store2 = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => databaseName }))
                    {
                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(8, stats.CountOfRevisionDocuments);

                        await AssertSupportedFeaturesAsync(store2, oldVersion: true);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        public async Task RestoreFromBackupWithDatabaseVersionShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                // convert the JSON database record into a memory stream
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(NewJsonRecord)))
                await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    zipStream.Flush();
                    ms.Position = 0;

                    // create a backup path and write the compressed stream to a file
                    var backupPath = NewDataPath(suffix: "BackupFolder", forceCreateDir: true);
                    var filePath = Path.Combine(backupPath, "test111.ravendb-full-backup");
                    await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                    {
                        await ms.CopyToAsync(fileStream); // Copy the contents of the memory stream to the file
                    }

                    var databaseName = "test111";
                    var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName };

                    // expect an exception to be thrown during the restore due to a large revision change vector
                    var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        var restoreOperation = new RestoreBackupOperation(restoreConfig);
                        var o = await store.Maintenance.Server.SendAsync(restoreOperation);
                        await o.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });

                    Assert.Contains(ExceptionMessage, exception.Message);
                }

                await AssertSupportedFeaturesAsync(store, oldVersion: false);
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        public async Task RestoreFromSnapshotShouldNotThrow()
        {
            // This should work regardless of the database version, because we're not iterating over documents
            // and inserting them into storage. Instead, we're restoring directly from the Voron file,
            // so the database version does not affect the outcome

            using (var store = GetDocumentStore())
            {
                var backupPath = NewDataPath(forceCreateDir: true);
                var fullBackupPath = Path.Combine(backupPath, "test111.ravendb-snapshot");
                var snapshotName = "revisionsWithLargeChangeVector-oldDatabaseVersion.ravendb-snapshot";

                await using (var file = File.Create(fullBackupPath))
                await using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_21263." + snapshotName))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }

                var databaseName = "test111";
                var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var store2 = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => databaseName }))
                {
                    var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                    Assert.Equal(8, stats.CountOfRevisionDocuments);

                    await AssertSupportedFeaturesAsync(store2, oldVersion: true);
                }
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task RestoreFromSnapshotShouldNotThrow_Cluster()
        {
            // This should work regardless of the database version, because we're not iterating over documents
            // and inserting them into storage. Instead, we're restoring directly from the Voron file,
            // so the database version does not affect the outcome

            var cluster = await CreateRaftCluster(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var backupPath = NewDataPath(forceCreateDir: true);
                var fullBackupPath = Path.Combine(backupPath, "test111.ravendb-snapshot");
                var snapshotName = "revisionsWithLargeChangeVector-oldDatabaseVersion.ravendb-snapshot";

                await using (var file = File.Create(fullBackupPath))
                await using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_21263." + snapshotName))
                {
                    Assert.NotNull(stream);
                    await stream.CopyToAsync(file);
                }

                var leaderTag = cluster.Leader.ServerStore.NodeTag;
                var databaseName = "test111";
                var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                var operation = await store.Maintenance.Server.ForNode(leaderTag).SendAsync(restoreOperation);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                // after restoration, ensure replication across cluster nodes
                foreach (var node in cluster.Nodes)
                {
                    if (leaderTag == node.ServerStore.NodeTag)
                        continue;

                    await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName, node.ServerStore.NodeTag));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Equal(3, record.Topology.Count);

                Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, databaseName));

                foreach (var node in cluster.Nodes)
                {
                    // validate that each cluster node does not contain 'ThrowRevisionKeyTooBigFix'
                    record = await store.Maintenance.Server.ForNode(node.ServerStore.NodeTag).SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.False(record.SupportedFeatures.Contains("ThrowRevisionKeyTooBigFix"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        public async Task ReduceChangeVectorByAddingUnusedIDsShouldFixPutRevision()
        {
            using (var store = GetDocumentStore())
            {
                var jsonString = OldJsonRecord;
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
                await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    zipStream.Flush();
                    ms.Position = 0;


                    // create a backup path and write the compressed stream to a file
                    var backupPath = NewDataPath(suffix: "BackupFolder", forceCreateDir: true);
                    var filePath = Path.Combine(backupPath, "test111.ravendb-full-backup");
                    await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write))
                    {
                        await ms.CopyToAsync(fileStream); // Copy the contents of the memory stream to the file
                    }

                    var databaseName = "test111";
                    var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName };

                    var restoreOperation = new RestoreBackupOperation(restoreConfig);
                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    using (var store2 = GetDocumentStore(new Options { CreateDatabase = false, ModifyDatabaseName = _ => databaseName }))
                    {
                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(8, stats.CountOfRevisionDocuments);

                        await AssertSupportedFeaturesAsync(store2, oldVersion: true);

                        using (var store3 = GetDocumentStore())
                        {
                            await RevisionsHelper.SetupRevisions(store3, Server.ServerStore);

                            // we expect to get the revision with the large change vector to store3 because it is from replication
                            await SetupReplicationAsync(store2, store3);
                            await EnsureReplicatingAsync(store2, store3);

                            // because store3 has a newer database version we should get exception (not from replication)
                            var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                            {
                                using (var session = store3.OpenAsyncSession())
                                {
                                    await session.StoreAsync(new Order(), "orders/5-A");
                                    await session.SaveChangesAsync();
                                }
                            });

                            Assert.Contains(ExceptionMessage, exception.Message);

                            var unusedDatabaseIds = new HashSet<string>
                            {
                                "46A/cZk8/UuwYdG5Dji0Sg",
                                "C3U1veikQEKrM2RUNtZQ5A",
                                "DxzjyxXS1EqgSEO3J5hgpw",
                                "K8YhRiUCo0GzECyk/JJeKw",
                                "MJQwORy0mkes5E53+IAkrg",
                                "Mgugu/MVqEOgJNafGyKIfg",
                                "QRjh5tTLokWSvDJDbF6JeQ",
                                "S1HWITSRQkSzFIy40y9ayg",
                                "UjG7submxUSZV+UH3Dx3+g",
                                "VszrCLgjjE+jfH1HCyLRSA",
                                "ekC7E4AuOU2O8fgRey3iJA",
                                "ez7yDfG04k+VpltCkY5frw",
                                "kd+eerHmN0ulK2L+gCPIeA",
                                "l1IVYfwRNUWjea1mFGMYhg",
                                "pX0Cwmvv50WIRxf45m2MtQ",
                                "sb+TC4f3XEGcC7vnkjCTrA"
                            };

                            // send the unused IDs to the server to reduce the change vector size
                            await store3.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(store3.Database, unusedDatabaseIds));

                            await AssertWaitForValueAsync(async () =>
                            {
                                var record = await store3.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store3.Database));
                                return record.UnusedDatabaseIds.Count;
                            }, unusedDatabaseIds.Count);

                            // now, the same operation should not throw an error
                            using (var session = store3.OpenAsyncSession())
                            {
                                await session.StoreAsync(new Order(), "orders/5-A");
                                await session.SaveChangesAsync();
                            }
                        }
                    }
                }
            }
        }

        private async Task AssertSupportedFeaturesAsync(DocumentStore store, bool oldVersion)
        {
            var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            if (oldVersion)
                Assert.True(databaseRecord.SupportedFeatures == null || databaseRecord.SupportedFeatures.Count == 0);
            else
                Assert.True(databaseRecord.SupportedFeatures.Contains(Raven.Client.Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix));
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_21263).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
