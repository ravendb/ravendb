using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19529 : ReplicationTestBase
{
    public RavenDB_19529(ITestOutputHelper output) : base(output)
    {
        Assert.True(SpecificContentWithSpecificSize.Last() != CharToAppend);
    }

    // We want to reproduce very narrow conditions under which a problem occurs:
    // 1. The document should contain random data that is difficult to compress.
    // 2. After compression, the compressed buffer should not fit into the small section.
    // 3. The size of the compressed buffer + sizeof(RawDataSection.RawDataEntrySizes) (4 byte) should be greater than RawDataSection.MaxItemSize (4064 bytes).
    // 4. We want to perform a merged write transaction, within which the opening of a table of a certain collection occurs first, and then a PUT (update) command
    //    is executed on the document specified above.

    private const string SpecificContentWithSpecificSize =
        "fR5WC1L5gHHLyuHA4\"O7/S/WUg:GXt5ViwqiPzfFoh\"k36lkf0tem\"3Ny91WM/m5zN1XfS0OIaTqdSnWeg2HfK9KDI3CG9P2qR8BTp8rdUsFSdv1n9GRyzyFrQbv5vLkh:g4Rfhuccy7Xl7OTj1nwTm7VU" +
        "6SFVkUYC8RzdWgAwl6:42KXOWTxh8pma:73CCJz5edl7IPWMR2dSoxymRNAQI7uPdIKJ7wbIUuwIlqSV9D\"cIPQp0Iexa7d2dEG0/97JCsPdBP8zPvQwbfSEjGO/iGLMdPFTXMlRt/PaRkh7jxVWTb7zpW4F" +
        "ZlW/YpJ4OyW\"aML73pjUhr6Qn7al30XqP9LgbufTF\"BPGsCF9DGgavUDuQnFhwZ8yvDrKugF7Zq/HHMevHREbREmu7dA:Cv7RqVtRoCQNZ75s\"MJnTPRYG5r1UACeiCAHAUhRJ:foH9IY3g5roeta707zC" +
        "1VQE5fJl9fUdbtz0t/vtecVbwL17rXGe7K/HxGcGyq9HgcLz6Z8\"lAHCaROu6fn3anjZkSwqpeOXpTlBHyrbpTuYVzfcmTASp\"oU4:yirNN28KUXirZpz6nBftuTf1gssK2jfR89ELiM1vyVcFozJ7iDwjm" +
        "l2Q4FLS\"AfK7yUWuN7QDEkySGfyPBKMdiOt0GY4OT2jLtUD6MMr8MDIfUIeTQXF3A18S2x3SaNFvDGqWhYkxUQdWru6QSPo9gR\"ziwhwwbvbYlxVZz76e\"h:gFGooCVI3IsxjBpxOgk:YQFnSeFXH1gmdt" +
        "VJOiX7V1DSCmbpcD/lrTLYzudQbAntOZ4MuavmWupoY2ujhYVwKls:sa/U7Kkl2jKL1es/o1iDdWu3joiQe/U3vDO4r38REawx9Rlzb6XhuJaXBP38BgqlLhm1STiWgDNms9LMVzD:2xj9Ibw1u/t0aleA7HC" +
        "yyEn0O:VgTvPlzNsl34wzveWy2ofvgMVMnfC9xYIZG2lCT3H2cnigadbhjCbPM8cURs/37E0ZDWn2OaaruEIUECdfcD:8Q8RVkP3f:g2cQ9UJr820QqDPPQY:H2IIUNt71sgyN0b6VTxwJQ\"6njuKjn5i1Qa" +
        "UDUsdqayMkIf/3zBT1cthyGs1M2sbqZaRMicIg8gvB9o:OCvcxsxiLcLwWCnqeBnk8VJSdU\"WavfQT/KvOMj3VroKre22w19eq7fentFkeKjJ4nQdlS5HdEHYYTIGhlMvU7pavuU8aaD3f511PBpdxDdSxm9" +
        "/i:AxsJKFOgRXJxr3RP9zylvumpTfqY7t:Ta9rSVePmzm\"p1AAHICJPn0OWZoDF4qE/0ObKObWT/4BClaDmSuRCu7OSfKyaC4CPQEjgfoHVLlRobMP5WRaUsu4sAEKX3f9oIn:eiwuwvv554nQYzUH8uAHz8" +
        "iCAl7oKuVnE1bdO5ym8Pu1AlqRRrZEWEaKrWZEAo\"0/9CsGXU9an0CnV6Bm5gDP7WYhjfBGZoRzNx9tHUurcYHFct0L/pEBcHMzumz115g98ZSJQ41XfL9mlxINfH5fqiG/FTi7rqpW9JGWPv3A22Qnbga\"" +
        "Ey2LBRFHGSwBihHFqtrHfXwCScUlidbQV3j8Kp9muS:Pb7sbgPkfiojB:KOddvc82QcHb1Z6PjTnPpZWPKffA9rim2drNu2hE/AOxLGCTI1IWgwPPkLUR1k/An3hGEStY1rvBLUW8hVi4PtXUs:8DVSNdG2AS" +
        "72bknpksssKwVMd\"GH8yyapWcKFnwXZsNmosxf/lg0qP0jlApazTtYDA1HDYprTKUwzhtrv7psYFy/TpM4X1bhc5TzWAF2bokD4D5aTdVgQJFqgnHMHwOpMZ:mUyuzKbg2Vc1:5S:zgvnR7GZ6/h5ST:HwVc" +
        "YLFfqPe3GkGoY6:t6IIeKCtS0Mnojftexso7bm5xiVKbCSb2/HC\"/vUhllKU3TPrIKiiXNq17nJryJM6l:CjkrhNDO23QUVnoXr2CveXO4:/TnOtuoEbvo4tV2F7S6l3n/cjFQz0qGaNtTq/9w7L1Sarw1JO" +
        ":aydw1wHgBDLuRe9TnQTmTeaVYl0cJtV2vB17ph4:21EHQ:2iKUnUlEWobMC3MQga6\"z\"VZao//tohGG3FBNtWM7QUjuDQ9HenZMDonWpvkHsoB1/h6KNbpnRsJnl4Geh5dr7qPNptPbHdfRGUkaluCkANI" +
        "09HhIvHfKFAUZXfYgsAmNORkdQ363fWLBfELSUhjB:zfS253zvgPt:zYLfqhzPkay0HClVg8esEPNQHTOTnE4MqULVYi3D0cUg2\"5lN2WVLQRnLthO/bhf\"1gam02FoBa1kbVry6kKxsyXmoK3RsZtVZ4vt" +
        "Tbjz:QNW5SWZuvRuGOW6AR7N4PquXDk/2t9dPED/DiazjPtD6Cp4gtSGKR6gzN/3EFMl8FB4etegShjLGD8KVRyAMO5lBhtWD10lmQAxIlLJJOO6hEMW3wlHSj5k/dGtwgQM29MTqfn\"3jG9PR:WJWwXNYJd" +
        "BD5eD7nYfXOidXp0Hpbp89h\"AMpECD52Zh\"zMq44emubBMN411Fvxq6DrDKgXaOTUMjOvNvQb5rQpRZ:txTb58SqR9QtxINq6G0K3gxaPjw3fJbDySUbVYUYu4def6iS6iywmrinpTf5P2vDK4toP:vECtu" +
        "Emb89uZj6EtDiW8:G1RJ9ony7g9InLmTqVd7pncd:FV5uaF:/bbjZPhH7PHZyZsZEiAW3lR8lb3sqOyDQQKk:l1jPU7DEO4llDO46hi\"LitTEZZ7\"Li6pv0uowZ1bL2XFI0XqGZPab\"mMh:c/QZr0aJwyl" +
        "xytDbk1QeI0dnlpFzJOYEJ3gEBMcYqtKIQfAKyCx09aiW\"aHCsSZvczKp5JTtmDYSCEbdf6aOW1quJ3y5efucJhEqwJzkFV:ENRsQmjn9\"NiC\"/0X\"CsG7MplWKjQw0Z\"Ol:lY2xEKofNlX5MmAdC8cS" +
        "t8PT8IsWfEh2ZL9pIr/MBaSNd1wathGPoDfH:8cHQ9JO\"QOwkZXIvZLgaGL4NKvGsiJ4rMI/CpV3mWcJYLslPowwQwaSMZnLizi4dXmP30sqZOeldUvzh9VS9G1tsCdgk53uIuwY4K7MXlfJCEOrUXqm\"wZ" +
        "SunvuQCTjdBYK4WA6J8/FYw2zO:aCbTcvDtoviouQIY\"/AM2nnS1hDJJRAEykFNvb9bCnf9ecnwDQ35pfEMB5ejnARwCRtSXxS2ULltAudbtPXMgLjEVkgy1UTfm8FotyI:9JzSuGsFFSF:j4qqh0RpnRx1T" +
        "Qrjbr6d\"yTDpS9/k16:ntThk5ak7JOl9e/9idEW6S:mSRqup:mjxF3XmXQyLgGj6/vvdrZC13mA6bPEH043xs7cdebZaAks2b4MBqXYn1Hf2M7bcj5:G6bAeW8NuV2tFUMDk4Vy6otT\"tNvg62kKVchtIAD" +
        "iJJY4vRHQRsE6BK2YfEOQtrVEzLpxiSssfwBu:ZgTq3/o1B9ojhXC\"iMVASO6ZwCzuxG0XZtU93bol4ab9SlyJNmBgcWE1AxQGvixt7S3OIXqFSmC0G69l1wyPEHyMH6Phpn:6m\"8ytDNtvUr9rsB3T5SwM" +
        "XlLN6QbzGqVTs3PKhCUPyMH6I:pBN1\"TRD1sQmqDWFE0toaDYIEEiXUZMRfbmyMt/QTWA6:aKboq/\"58e2IQayOofUHLf111vlk55hiEePdWEK/R3ObMSrG7X6DJr6aHk\"7x8Axn\"vUSDH6D4KBnGcG\"" +
        "T/V15bo:EfyCwg\"Fp9qepH8l:5UjNJkvRG7nCfXs02O8qp6QRYaIlz13fH3rs\"Ao/83n9G5kxNBTF/maIQzIEWaowQhHmJyuwucxUWuF1wbu1EbEgsrtXXD0w1OE7TDgxwqQQIjFprRAQTCXCyJmBS/7hyV" +
        "HSOQi4r\"PI\"vCW:8HxC1/106ddZaGjJ1L33I90ZcdRfPxuWqgFKAfeCtu6\"oFEIw3wQDbHVBSZ16AD5lY/WD64w3eYcpuM8MFIoidTRRYckYkO0\"jf:GS4IqtrZ:tcOxS1W\"WU6l\"Pn/ERKYEcV8caX" +
        "jdwJbyDkkYaZTulZFZEa9kgZNJhNfQWZwdar4sobzPUoio1epErRwxn4GCS6wpox:2x4lqMgE9tbLTpQBdudmhKcrSeYkwtF1mGmClW37FTLvM8ZnDoPkfk054P8US:Ek\"RpjIZ5Kx1bjo/jaH9pXIRHFtnF" +
        "18LVcdV96Lt0lrw35sdrmcnWh1jitWsM7PGblv41B9:n0/HM7TIynJ2PGCd9D\"ioY24ax1xHtORsJKNMg0YRHF1glpYxjp45C:saQ7PvXKYBJMpI8fnP4bjgl2h13Ha8l/BKh0EqgLrBQKQbystJn5XQlp:c" +
        "\"JCfLPVITpULwy/r7Ca7zngDaI30xo5M0Z2GPk:LDs4isfiZCKOVJcdiGuy36VXoBxaTIF6LE5lkfsrE5Nh60y:H7F/PP69gEq4Dm61D8CLpZiYFkKf1GSPWuzNVhAwG5q\"9QjPEN:AU8PgHs/GOBn2:wVl" +
        "1r5g1DWtDVAgXWRmxgTulqDH3B42xNx:wCMMRC38I0LjRu8gbFCb33sbATA4BPBlFLc3IojNAwdxrA9Vm1ey:AIgZel4NaCcDl/1KudpktFb3uQ29UJyI4UM26x5YTi63U8zJmPn\"u1SD31pJfsvV4wVTn1P" +
        "sQlralIbsuIMkySz3zT:CBiP6u5cnz8vjZiaefsvim0YKv6N/n\"NzmPLRTLd3onpZeunIzKeNqNsnkCFOijZY:B6yjMOPOF/xLklkYrEPr4YTPFomzQpj6wXr7WQhg02hFgkM;IjhHgTgHjhKllm,ngRdFDg";

    private const char CharToAppend = 'a';

    [RavenFact(RavenTestCategory.Compression | RavenTestCategory.Voron)]
    public async Task MergedTransaction_DeleteOneDoc_Then_PutAnotherToLargeSection_CompressionSetOnServerCreation()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToDeleteId = "users/2-A";

        using (var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.CompressAllCollectionsDefault)] = true.ToString(),
            }
        }))
        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                await session.StoreAsync(new User { Name = "Document To Delete" }, documentToDeleteId);

                await session.SaveChangesAsync();
            }

            using (var operationSession = store.OpenSession())
            {
                // Form a merge transaction within which there will first be a DELETE command...
                var userToDelete = operationSession.Load<User>(documentToDeleteId);
                operationSession.Delete(userToDelete);

                // ...and then a PUT command
                var specificSizeAndContentUser = operationSession.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;

                operationSession.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(store, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't updated as expected");

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, specificSizeAndContentDocumentId, out Slice lowerDocumentId))
            {
                Assert.True(database.DocumentsStorage.ForTestingPurposesOnly().IsDocumentCompressed(context, lowerDocumentId, out var isLargeValue));
                Assert.True(isLargeValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.Compression | RavenTestCategory.Voron)]
    public async Task MergedTransaction_DeleteOneDoc_Then_PutAnotherToLargeSection_CompressionSetInTheMiddle()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToDeleteId = "users/2-A";

        using (var server = GetNewServer())
        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                await session.StoreAsync(new User { Name = "Document To Delete" }, documentToDeleteId);

                await session.SaveChangesAsync();
            }

            store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration { CompressAllCollections = true }));

            using (var operationSession = store.OpenSession())
            {
                // Form a merge transaction within which there will first be a DELETE command...
                var userToDelete = operationSession.Load<User>(documentToDeleteId);
                operationSession.Delete(userToDelete);

                // ...and then a PUT command
                var specificSizeAndContentUser = operationSession.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;

                operationSession.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(store, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't updated as expected");

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, specificSizeAndContentDocumentId, out Slice lowerDocumentId))
            {
                Assert.True(database.DocumentsStorage.ForTestingPurposesOnly().IsDocumentCompressed(context, lowerDocumentId, out var isLargeValue));
                Assert.True(isLargeValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.Compression | RavenTestCategory.Voron)]
    public async Task MergedTransaction_AddConflict_Then_PutUpdateDocumentOnLargeSection_CompressionSetInTheMiddle()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToConflictId = "users/2-A";

        using (var storeSrc = GetDocumentStore())
        using (var storeDst = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
            }
        }))
        {
            storeDst.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration { CompressAllCollections = true }));

            using (var sessionSrc = storeSrc.OpenSession())
            {
                sessionSrc.Store(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                sessionSrc.SaveChanges();
            }

            var replicationTasks = await SetupReplicationAsync(storeSrc, storeDst);
            var taskId = replicationTasks.First().TaskId;
            Assert.NotNull(WaitForDocumentToReplicate<User>(storeDst, specificSizeAndContentDocumentId, 15000));

            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: true));

            using (var sessionSrc = storeSrc.OpenSession())
            {
                // To create a merge transaction that will be formed after enabling replication, we first need to create a Conflict...
                sessionSrc.Store(new User { Name = "Document To Conflict" }, documentToConflictId);

                // ...and then a PUT command
                var specificSizeAndContentUser = sessionSrc.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;

                sessionSrc.SaveChanges();
            }

            using (var sessionDst = storeDst.OpenSession())
            {
                sessionDst.Store(new User { Name = "Another Document To Conflict" }, documentToConflictId);
                sessionDst.SaveChanges();
            }

            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: false));

            WaitUntilHasConflict(storeDst, documentToConflictId, count: 1);
            Assert.True(WaitForDocument<User>(storeDst, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't replicated as expected");

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(storeDst.Database);
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, specificSizeAndContentDocumentId, out Slice lowerDocumentId))
            {
                Assert.True(database.DocumentsStorage.ForTestingPurposesOnly().IsDocumentCompressed(context, lowerDocumentId, out var isLargeValue));
                Assert.True(isLargeValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.Compression | RavenTestCategory.Voron)]
    public async Task CanHandleChangingCompressionConfigurationInTheMiddleOfTransaction()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToDeleteId = "users/2-A";

        using (var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.CompressAllCollectionsDefault)] = true.ToString(),
            }
        }))
        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                await session.StoreAsync(new User { Name = "Document To Delete" }, documentToDeleteId);

                await session.SaveChangesAsync();
            }

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            database.DocumentsStorage.ForTestingPurposesOnly().OnBeforeOpenTableWhenPutDocumentWithSpecificId = id =>
            {
                if (id == specificSizeAndContentDocumentId)
                    store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration { CompressAllCollections = false }));
            };

            using (var operationSession = store.OpenSession())
            {
                // Form a merge transaction within which there will first be a DELETE command...
                var userToDelete = operationSession.Load<User>(documentToDeleteId);
                operationSession.Delete(userToDelete);

                // ...and then a PUT command
                var specificSizeAndContentUser = operationSession.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;

                operationSession.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(store, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't updated as expected");

            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, specificSizeAndContentDocumentId, out Slice lowerDocumentId))
            {
                Assert.False(database.DocumentsStorage.ForTestingPurposesOnly().IsDocumentCompressed(context, lowerDocumentId, out var isLargeValue));
                Assert.True(isLargeValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
    public async Task MergedTransaction_PutArchivedDoc_Then_PutAnotherToLargeSection_CompressionSetInTheMiddle()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToArchiveId = "users/2-A";

        using (var storeSrc = GetDocumentStore())
        using (var storeDst = GetDocumentStore())
        {
            var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 1 };
            await DataArchivalHelper.SetupDataArchival(storeSrc, Server.ServerStore, config);
            await DataArchivalHelper.SetupDataArchival(storeDst, Server.ServerStore, config);

            using (var sessionSrc = storeSrc.OpenSession())
            {
                sessionSrc.Store(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                sessionSrc.SaveChanges();
            }

            var taskId = SetupReplicationAsync(storeSrc, storeDst).Result.First().TaskId;
            Assert.NotNull(WaitForDocumentToReplicate<User>(storeDst, specificSizeAndContentDocumentId, 15000));

            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: true));

            using (var sessionSrc = storeSrc.OpenAsyncSession())
            {
                await sessionSrc.StoreAsync(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);

                var docToArchive = new User { Name = "Document To Archive" };
                await sessionSrc.StoreAsync(docToArchive, documentToArchiveId);

                await sessionSrc.SaveChangesAsync();

                var archiveDateTime = SystemTime.UtcNow;
                var metadata = sessionSrc.Advanced.GetMetadataFor(docToArchive);
                metadata[Constants.Documents.Metadata.ArchiveAt] = archiveDateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await sessionSrc.SaveChangesAsync();
            }

            var databaseSrc = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(storeSrc.Database);
            await databaseSrc.DataArchivist.ArchiveDocs();

            using (var sessionSrc = storeSrc.OpenSession())
            {
                // ...and then a PUT command
                var specificSizeAndContentUser = sessionSrc.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;

                sessionSrc.SaveChanges();
            }

            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: false));

            Assert.True(WaitForDocument<User>(storeDst, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't updated as expected");

            var databaseDst = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(storeDst.Database);

            using (var context = DocumentsOperationContext.ShortTermSingleUse(databaseDst))
            using (context.OpenReadTransaction())
            using (DocumentIdWorker.GetSliceFromId(context, specificSizeAndContentDocumentId, out Slice lowerDocumentId))
            {
                Assert.False(databaseDst.DocumentsStorage.ForTestingPurposesOnly().IsDocumentCompressed(context, lowerDocumentId, out var isLargeValue));
                Assert.True(isLargeValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.Compression | RavenTestCategory.Voron,
        Skip = "Conflict for for document in different collections can be resolved only manually - Issue RavenDB-17382")]
    public async Task MergedTransaction_ConflictForDocumentInDifferentCollection_Then_PutUpdateDocumentOnLargeSection()
    {
        const string specificSizeAndContentDocumentId = "users/1-A";
        const string documentToConflictId = "users/2-A";

        using (var storeSrc = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
            }
        }))
        using (var storeDst = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
            }
        }))
        {
            storeDst.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration { CompressAllCollections = true }));

            using (var session = storeSrc.OpenSession())
            {
                session.Store(new User { Name = SpecificContentWithSpecificSize }, specificSizeAndContentDocumentId);
                session.SaveChanges();
            }

            var replicationTasks = await SetupReplicationAsync(storeSrc, storeDst);
            var taskId = replicationTasks.First().TaskId;
            Assert.NotNull(WaitForDocumentToReplicate<User>(storeDst, specificSizeAndContentDocumentId, 15000));

            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: true));

            using (var sessionDst = storeDst.OpenSession())
            {
                sessionDst.Store(new User { Name = "Document To Conflict" }, documentToConflictId);
                sessionDst.SaveChanges();
            }

            using (var sessionSrc = storeSrc.OpenSession())
            {
                // To create a merge transaction that will be formed after enabling replication, we first need to create a Conflict in different collection...
                sessionSrc.Store(new Company { Name = "Another Document To Conflict" }, documentToConflictId);

                // ...and then a PUT command
                var specificSizeAndContentUser = sessionSrc.Load<User>(specificSizeAndContentDocumentId);
                specificSizeAndContentUser.Name += CharToAppend;
                sessionSrc.SaveChanges();
            }

            await SetReplicationConflictResolutionAsync(storeDst, StraightforwardConflictResolution.ResolveToLatest);
            await storeSrc.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, disable: false));
            Assert.True(WaitForDocument<User>(storeDst, specificSizeAndContentDocumentId, user => user.Name.Last() == CharToAppend),
                $"The document '{specificSizeAndContentDocumentId}' wasn't replicated as expected");
        }
    }
}
