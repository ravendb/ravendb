using System;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Expiration;
using FastTests.Server.Documents.Indexing.Auto;
using FastTests.Server.Documents.Replication;
using FastTests.Voron;
using Raven.Abstractions.Extensions;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                Console.Write(".");
                using (var a = new BasicAutoMapReduceIndexing())
                using (var b = new Expiration())
                using (var c = new ReplicationTombstoneTests())
                using (var p = new RavenDB5743())
                {
                    Task[] tasks =
                    {
                        a.MultipleReduceKeys(50000, new string[] { "Canada", "France" }),
                        b.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(10),
                        c.Tombstone_should_replicate_in_master_master(),
                        p.WillFilterMetadataPropertiesStartingWithAt(),

                        b.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(10),
                        c.Tombstone_should_replicate_in_master_master(),
                        p.WillFilterMetadataPropertiesStartingWithAt(),

                        b.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(10),
                        c.Tombstone_should_replicate_in_master_master(),
                        p.WillFilterMetadataPropertiesStartingWithAt(),

                        b.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(10),
                        c.Tombstone_should_replicate_in_master_master(),
                        p.WillFilterMetadataPropertiesStartingWithAt(),

                        b.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(10),
                        c.Tombstone_should_replicate_in_master_master(),
                        p.WillFilterMetadataPropertiesStartingWithAt(),

                    };




                    Task.WaitAll(tasks);
                }
            }
        }
    }


}

