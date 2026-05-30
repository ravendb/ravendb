using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class ChangeVectorConflictStatusTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Replication)]
        public void EtagShouldNotOverflow()
        {
            var cv1 =
                "A:86865297-V8jm+M9QKkuvfEUTQBfOtA, " +
                "C:87142328-5j4moMb8A0KxxcL9GhY/nw, " +
                "B:2146533895-SKM7aNMmSkW92wrQke+D4g, " +
                "E:1856361198-/mqfiL1AxkGlsqx1zwh2rw, " +
                "D:1882901489-TqJlheobc0KTcLDerIQ9oQ, " +
                "D:17267243-/3+4WZUBGkWL6/J4GMv2GA, " +
                "D:46103608-P1lQdjeAckGkdmY9RWr/Bg, " +
                "A:27850500-iUMDTgYwOkG25uod1g6gSg";

            var cv2 =
                "C:87142328-5j4moMb8A0KxxcL9GhY/nw, " +
                "B:2146533895-SKM7aNMmSkW92wrQke+D4g, " +
                "E:1856361198-/mqfiL1AxkGlsqx1zwh2rw, " +
                "D:1882901489-TqJlheobc0KTcLDerIQ9oQ, " +
                "A:27850500-iUMDTgYwOkG25uod1g6gSg, " +
                "A:86865297-V8jm+M9QKkuvfEUTQBfOtA, " +
                "A:2319854662-eCGjjCNbP0CeTGSJMeqLZA";

            ChangeVectorUtils.MergeVectors(cv1, cv2).ToChangeVector();
            var x = ChangeVectorUtils.Distance(cv1, cv2);
            var y = ChangeVectorUtils.Distance(cv2, cv1);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void CalculateChangeVectorDistance()
        {
            var cv1 =
                "A:86865297-V8jm+M9QKkuvfEUTQBfOtA, " +
                "C:87142328-5j4moMb8A0KxxcL9GhY/nw, " +
                "B:2146533895-SKM7aNMmSkW92wrQke+D4g, " +
                "E:1856361198-/mqfiL1AxkGlsqx1zwh2rw, " +
                "D:1882901489-TqJlheobc0KTcLDerIQ9oQ, " +
                "D:17267243-/3+4WZUBGkWL6/J4GMv2GA, " +
                "D:46103608-P1lQdjeAckGkdmY9RWr/Bg, " +
                "A:27850500-iUMDTgYwOkG25uod1g6gSg";

            var cv2 =
                "C:87142328-5j4moMb8A0KxxcL9GhY/nw, " +
                "B:2146533895-SKM7aNMmSkW92wrQke+D4g, " +
                "E:1856361198-/mqfiL1AxkGlsqx1zwh2rw, " +
                "D:1882901489-TqJlheobc0KTcLDerIQ9oQ, " +
                "A:27850500-iUMDTgYwOkG25uod1g6gSg, " +
                "A:86865297-V8jm+M9QKkuvfEUTQBfOtA, " +
                "A:2319854662-eCGjjCNbP0CeTGSJMeqLZA";

            var x = ChangeVectorUtils.Distance(cv1, cv2);
            var y = ChangeVectorUtils.Distance(cv2, cv1);

            Assert.Equal(x, -y);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void EtagShouldNotOverflow2()
        {
            var x = ChangeVectorUtils.TryUpdateChangeVector("C", "n0rGjcmUT0u7ctxBXlZZPg", 5554138256, new ChangeVector("C:5554138256-n0rGjcmUT0u7ctxBXlZZPg", null));

            Assert.False(x.IsValid);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Two_empty_ChangeVectors()
        {
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, string.Empty));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(null, string.Empty));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, null));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Empty_remote_change_vector_should_generate_already_merged()
        {
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2))));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Empty_local_change_vector()
        {
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2)), string.Empty));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Change_vector_has_negative_etag()
        {
            var changeVectorWithNegatoveEtag = ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), -3, 2));
            var changeVector = ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2));

            Assert.Throws<ArgumentException>(() =>
                ChangeVectorUtils.GetConflictStatus(changeVectorWithNegatoveEtag, changeVector));

            Assert.Throws<ArgumentException>(() =>
                ChangeVectorUtils.GetConflictStatus(changeVector, changeVectorWithNegatoveEtag));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_all_remote_etags_large_than_local()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]), (dbIds[2], 5, tags[2]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_all_remote_etags_large_than_local()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 5, tags[2]), (dbIds[1], 5, tags[1]), (dbIds[0], 5, tags[0]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_all_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_all_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 1, tags[2]), (dbIds[1], 1, tags[1]), (dbIds[0], 1, tags[0]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_some_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 5, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_some_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 1, tags[2]), (dbIds[1], 5, tags[1]), (dbIds[0], 1, tags[0]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_equal_length_same_order_should_work(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0; i < length; i++)
            {
                remoteVectorData.Add((dbIds[i], 10, tags[i]));
                localVectorData.Add((dbIds[i], 1, tags[i]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_longer_same_order_should_work_all_remote_etags_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0; i < length; i++)
            {
                remoteVectorData.Add((dbIds[i], 10, tags[i]));
                if (length - i >= 5)
                {
                    localVectorData.Add((dbIds[i], 1, tags[i]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_longer_same_order_should_work_all_remote_etags_smaller(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0; i < length; i++)
            {
                remoteVectorData.Add((dbIds[i], 1, tags[i]));
                if (length - i >= 5)
                {
                    localVectorData.Add((dbIds[i], 10, tags[i]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_longer_same_order_should_work_some_remote_etags_smaller_and_some_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0; i < length; i++)
            {
                remoteVectorData.Add((dbIds[i], i % 3 == 0 ? 10 : 1, tags[i]));
                if (length - i >= 5)
                {
                    localVectorData.Add((dbIds[i], i % 2 == 0 ? 10 : 1, tags[i]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_local_longer_same_order_should_work(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0; i < length; i++)
            {
                localVectorData.Add((dbIds[i], 1, tags[i]));
                if (length - i >= 5)
                {
                    remoteVectorData.Add((dbIds[i], 10, tags[i]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_equal_length_different_order_should_work_all_remote_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], 10, tags[i]));
                localVectorData.Add((dbIds[j], 1, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }


        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_equal_length_different_order_should_work_all_local_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], 1, tags[i]));
                localVectorData.Add((dbIds[j], 10, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_equal_length_different_order_should_work_some_local_larger_some_smaller(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], i % 4 == 0 ? 11 : 1, tags[i]));
                localVectorData.Add((dbIds[j], 10, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_longer_different_order_should_work_some_local_larger_some_smaller(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], i % 2 == 0 ? 11 : 1, tags[i]));

                if (j >= 10)
                {
                    localVectorData.Add((dbIds[j], 10, tags[j]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_local_longer_different_order_should_work_some_local_larger_some_smaller(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                if (length - i >= 10)
                {
                    remoteVectorData.Add((dbIds[i], i % 4 == 0 ? 11 : 1, tags[i]));
                }

                localVectorData.Add((dbIds[j], 10, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_larger_different_order_should_work_all_local_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], 1, tags[i]));
                if (j >= 10)
                {
                    localVectorData.Add((dbIds[j], 10, tags[j]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_local_larger_different_order_should_work_all_local_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                if (length - i >= 10) //missing entry is treated as if etag == 0
                {
                    remoteVectorData.Add((dbIds[i], 1, tags[i]));
                }

                localVectorData.Add((dbIds[j], 10, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_remote_larger_different_order_should_work_all_remote_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], 10, tags[i]));
                if (j >= 10)
                {
                    localVectorData.Add((dbIds[j], 1, tags[j]));
                }
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Very_large_change_vectors_local_larger_different_order_should_work_all_remote_larger(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                if (length - i >= 10)
                {
                    remoteVectorData.Add((dbIds[i], 10, tags[i]));
                }

                localVectorData.Add((dbIds[j], 1, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Only_one_etag_is_larger_at_remote(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], i == length / 2 ? 15 : 5, tags[i]));
                localVectorData.Add((dbIds[j], 10, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(15)]
        [InlineData(100)]
        [InlineData(1000)]
        public void Only_one_etag_is_larger_at_local(int length)
        {
            var dbIds = new List<Guid>();

            for (int i = 0; i < length; i++)
                dbIds.Add(Guid.NewGuid());

            var tags = Enumerable.Range(0, length).ToArray();

            //we create two change vectors, where remote >> local
            var remoteVectorData = new List<(Guid, long, int)>();
            var localVectorData = new List<(Guid, long, int)>();

            for (int i = 0, j = length - 1; i < length; i++, j--)
            {
                remoteVectorData.Add((dbIds[i], 10, tags[i]));
                localVectorData.Add((dbIds[j], i == length / 2 ? 15 : 5, tags[j]));
            }

            var remote = ChangeVector(remoteVectorData.ToArray());
            var local = ChangeVector(localVectorData.ToArray());

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Different_change_vectors_with_different_prefix_remote_smaller_with_remote_etags_smaller()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 10, tags[0]), (dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Different_change_vectors_with_different_prefix_local_smaller_with_remote_etags_smaller()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Different_change_vectors_with_different_prefix_remote_smaller_with_remote_etags_larger()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void Different_change_vectors_with_different_prefix_local_smaller_with_remote_etags_larger()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[0], 10, tags[0]), (dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            var local = ChangeVector((dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ToChangeVector_should_properly_parse_change_vector()
        {
            var dbIds = new List<string> { DbId(), DbId(), DbId() };
            dbIds = dbIds.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
            var changeVector = new[]
            {
                new ChangeVectorEntry
                {
                    DbId = dbIds[0],
                    Etag = 1,
                    NodeTag = 0
                },
                new ChangeVectorEntry
                {
                    DbId = dbIds[1],
                    Etag = 1,
                    NodeTag = 1
                },
                new ChangeVectorEntry
                {
                    DbId = dbIds[2],
                    Etag = 1,
                    NodeTag = 2
                }
            };

            var changeVectorAsString = changeVector.SerializeVector();
            var parsedChangeVector = changeVectorAsString.ToChangeVector();

            for (int i = 0; i < parsedChangeVector.Length; i++)
            {
                Assert.Equal(parsedChangeVector[i], changeVector[i]);
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsShouldMergeCompositeOrderAndVersionSeparately()
        {
            var sourceDbId = Guid.NewGuid();
            var shard1DbId = Guid.NewGuid();
            var shard2DbId = Guid.NewGuid();

            var order1 = ChangeVector((shard1DbId, 500, 0));
            var order2 = ChangeVector((shard2DbId, 700, 1));
            var version1 = ChangeVector((sourceDbId, 100, 2));
            var version2 = ChangeVector((sourceDbId, 95, 2));

            var result = ChangeVectorUtils.MergeVectors($"{order1}|{version1}", $"{order2}|{version2}");

            Assert.Equal($"{ChangeVector((shard1DbId, 500, 0), (shard2DbId, 700, 1))}|{version1}", result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsShouldTreatMonoVectorAsBothOrderAndVersionWhenMixedWithComposite()
        {
            var sourceDbId = Guid.NewGuid();
            var shardDbId = Guid.NewGuid();

            var order = ChangeVector((shardDbId, 500, 0));
            var version = ChangeVector((sourceDbId, 100, 1));
            var mono = ChangeVector((sourceDbId, 110, 1));

            var result = ChangeVectorUtils.MergeVectors($"{order}|{version}", mono);

            Assert.Equal($"{ChangeVectorUtils.MergeVectors(order, mono)}|{mono}", result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsShouldKeepMonoOutputForMonoInputs()
        {
            var dbId = Guid.NewGuid();

            var result = ChangeVectorUtils.MergeVectors(ChangeVector((dbId, 100, 0)), ChangeVector((dbId, 110, 0)));

            Assert.DoesNotContain("|", result);
            Assert.Equal(ChangeVector((dbId, 110, 0)), result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsShouldMergeManyCompositeInputsSeparately()
        {
            var destinationDbId0 = Guid.NewGuid();
            var destinationDbId1 = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();

            var destination0At500 = ChangeVector((destinationDbId0, 500, 0));
            var destination0At650 = ChangeVector((destinationDbId0, 650, 0));
            var destination1At700 = ChangeVector((destinationDbId1, 700, 1));
            var sourceAt95 = ChangeVector((sourceDbId, 95, 2));
            var sourceAt100 = ChangeVector((sourceDbId, 100, 2));
            var sourceAt110 = ChangeVector((sourceDbId, 110, 2));

            var result = ChangeVectorMerger.Merge([
                $"{destination0At500}|{sourceAt100}",
                $"{destination1At700}|{sourceAt95}",
                $"{destination0At650}|{sourceAt110}"
            ]);

            Assert.Equal($"{ChangeVector((destinationDbId0, 650, 0), (destinationDbId1, 700, 1))}|{sourceAt110}", result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsDownShouldMergeCompositeOrderAndVersionSeparately()
        {
            var sourceDbId = Guid.NewGuid();
            var shardDbId = Guid.NewGuid();

            var result = ChangeVectorMerger.MergeDown([
                $"{ChangeVector((shardDbId, 500, 0))}|{ChangeVector((sourceDbId, 100, 1))}",
                $"{ChangeVector((shardDbId, 450, 0))}|{ChangeVector((sourceDbId, 95, 1))}"
            ], ChangeVectorPart.Whole);

            Assert.Equal($"{ChangeVector((shardDbId, 450, 0))}|{ChangeVector((sourceDbId, 95, 1))}", result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsDownShouldKeepUnmatchedEntriesFromFirstVectorWhenLaterVectorPartiallyOverlaps()
        {
            var dbA = Guid.NewGuid();
            var dbB = Guid.NewGuid();

            var result = ChangeVectorMerger.MergeDown([
                ChangeVector((dbA, 10, 0), (dbB, 20, 1)),
                ChangeVector((dbA, 8, 0))
            ], ChangeVectorPart.Whole);

            Assert.Equal(ChangeVector((dbA, 8, 0), (dbB, 20, 1)), result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVectorsDownShouldReturnNullWhenNoEntriesOverlap()
        {
            var dbA = Guid.NewGuid();
            var dbB = Guid.NewGuid();
            var dbC = Guid.NewGuid();

            var result = ChangeVectorMerger.MergeDown([
                ChangeVector((dbA, 10, 0), (dbB, 20, 1)),
                ChangeVector((dbC, 5, 2))
            ], ChangeVectorPart.Whole);

            Assert.Null(result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVersionVectorsDownShouldUseVersionFrontierOnly()
        {
            var sourceDbId = Guid.NewGuid();
            var shard1DbId = Guid.NewGuid();
            var shard2DbId = Guid.NewGuid();

            var version100 = ChangeVector((sourceDbId, 100, 2));
            var version95 = ChangeVector((sourceDbId, 95, 2));
            var changeVectors = new List<string>
            {
                $"{ChangeVector((shard1DbId, 500, 0))}|{version100}",
                $"{ChangeVector((shard2DbId, 700, 1))}|{version95}"
            };

            Assert.Null(ChangeVectorMerger.MergeDown(changeVectors, ChangeVectorPart.Whole));
            Assert.Equal(version95, ChangeVectorMerger.MergeDown(changeVectors, ChangeVectorPart.Version));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeVersionDownShouldTreatMonoVectorAsVersionWhenMixedWithComposite()
        {
            var sourceDbId = Guid.NewGuid();
            var destinationDbId = Guid.NewGuid();

            var sourceAt108 = ChangeVector((sourceDbId, 108, 0));
            var sourceAt104 = ChangeVector((sourceDbId, 104, 0));
            var destinationAt900 = ChangeVector((destinationDbId, 900, 1));

            var result = ChangeVectorMerger.MergeDown([
                sourceAt108,
                $"{destinationAt900}|{sourceAt104}"
            ], ChangeVectorPart.Version);

            Assert.Equal(sourceAt104, result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeOrderDownShouldUseOrderFrontierOnly()
        {
            var destinationDbId = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();

            var destinationAt900 = ChangeVector((destinationDbId, 900, 0));
            var destinationAt850 = ChangeVector((destinationDbId, 850, 0));
            var sourceAt104 = ChangeVector((sourceDbId, 104, 1));
            var sourceAt999 = ChangeVector((sourceDbId, 999, 1));

            var result = ChangeVectorMerger.MergeDown([
                $"{destinationAt900}|{sourceAt104}",
                $"{destinationAt850}|{sourceAt999}"
            ], ChangeVectorPart.Order);

            Assert.Equal(destinationAt850, result);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ExplicitEtagPartHelpersShouldReadOnlyRequestedCompositePart()
        {
            var orderDbId = Guid.NewGuid();
            var versionDbId = Guid.NewGuid();
            var missingDbIdString = Guid.NewGuid().AsChangeVectorDbId();
            var orderDbIdString = orderDbId.AsChangeVectorDbId();
            var versionDbIdString = versionDbId.AsChangeVectorDbId();

            var order = ChangeVector((orderDbId, 500, 0));
            var version = ChangeVector((versionDbId, 100, 1));
            var composite = $"{order}|{version}";

            Assert.Equal(500, ChangeVectorUtils.GetOrderEtagById(composite, orderDbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetOrderEtagById(composite, versionDbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetVersionEtagById(composite, orderDbIdString));
            Assert.Equal(100, ChangeVectorUtils.GetVersionEtagById(composite, versionDbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetOrderEtagById(composite, missingDbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetVersionEtagById(composite, missingDbIdString));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ExplicitEtagPartHelpersShouldTreatMonoChangeVectorAsOrderAndVersion()
        {
            var dbId = Guid.NewGuid();
            var dbIdString = dbId.AsChangeVectorDbId();
            var changeVector = ChangeVector((dbId, 100, 0));

            Assert.Equal(100, ChangeVectorUtils.GetOrderEtagById(changeVector, dbIdString));
            Assert.Equal(100, ChangeVectorUtils.GetVersionEtagById(changeVector, dbIdString));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ExplicitPartHelpersShouldReadSameDbIdFromRequestedCompositePartOnly()
        {
            var dbId = Guid.NewGuid();
            var dbIdString = dbId.AsChangeVectorDbId();
            var composite = $"{ChangeVector((dbId, 500, 0))}|{ChangeVector((dbId, 100, 1))}";

            Assert.Equal(500, ChangeVectorUtils.GetOrderEtagById(composite, dbIdString));
            Assert.Equal(100, ChangeVectorUtils.GetVersionEtagById(composite, dbIdString));
            Assert.Equal("A", ChangeVectorUtils.GetOrderNodeTagById(composite, dbIdString));
            Assert.Equal("B", ChangeVectorUtils.GetVersionNodeTagById(composite, dbIdString));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ExplicitNodeTagPartHelpersShouldReadOnlyRequestedCompositePart()
        {
            var orderDbId = Guid.NewGuid();
            var versionDbId = Guid.NewGuid();
            var missingDbIdString = Guid.NewGuid().AsChangeVectorDbId();
            var orderDbIdString = orderDbId.AsChangeVectorDbId();
            var versionDbIdString = versionDbId.AsChangeVectorDbId();

            var order = ChangeVector((orderDbId, 500, 0));
            var version = ChangeVector((versionDbId, 100, 1));
            var composite = $"{order}|{version}";

            Assert.Equal("A", ChangeVectorUtils.GetOrderNodeTagById(composite, orderDbIdString));
            Assert.Null(ChangeVectorUtils.GetOrderNodeTagById(composite, versionDbIdString));
            Assert.Null(ChangeVectorUtils.GetVersionNodeTagById(composite, orderDbIdString));
            Assert.Equal("B", ChangeVectorUtils.GetVersionNodeTagById(composite, versionDbIdString));
            Assert.Null(ChangeVectorUtils.GetOrderNodeTagById(composite, missingDbIdString));
            Assert.Null(ChangeVectorUtils.GetVersionNodeTagById(composite, missingDbIdString));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ExplicitPartHelpersShouldHandleNullEmptyAndNullId()
        {
            var dbIdString = Guid.NewGuid().AsChangeVectorDbId();

            Assert.Equal(0, ChangeVectorUtils.GetOrderEtagById(null, dbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetVersionEtagById(null, dbIdString));
            Assert.Null(ChangeVectorUtils.GetOrderNodeTagById(null, dbIdString));
            Assert.Null(ChangeVectorUtils.GetVersionNodeTagById(null, dbIdString));

            Assert.Equal(0, ChangeVectorUtils.GetOrderEtagById(string.Empty, dbIdString));
            Assert.Equal(0, ChangeVectorUtils.GetVersionEtagById(string.Empty, dbIdString));
            Assert.Null(ChangeVectorUtils.GetOrderNodeTagById(string.Empty, dbIdString));
            Assert.Null(ChangeVectorUtils.GetVersionNodeTagById(string.Empty, dbIdString));

            Assert.Throws<ArgumentNullException>(() => ChangeVectorUtils.GetOrderEtagById(string.Empty, null));
            Assert.Throws<ArgumentNullException>(() => ChangeVectorUtils.GetVersionEtagById(string.Empty, null));
            Assert.Throws<ArgumentNullException>(() => ChangeVectorUtils.GetOrderNodeTagById(string.Empty, null));
            Assert.Throws<ArgumentNullException>(() => ChangeVectorUtils.GetVersionNodeTagById(string.Empty, null));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void CompositeChangeVectorShouldRejectMultipleSeparators()
        {
            var dbId = Guid.NewGuid();
            var malformedComposite = $"{ChangeVector((dbId, 1, 0))}|{ChangeVector((dbId, 2, 1))}|{ChangeVector((dbId, 3, 2))}";

            Assert.Throws<ArgumentException>(() => ChangeVectorUtils.GetOrderEtagById(malformedComposite, dbId.AsChangeVectorDbId()));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void GetConflictStatusShouldUseVersionPartForCompositeStrings()
        {
            var destinationDbId = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();

            var remoteWithAdvancedOrderOnly = $"{ChangeVector((destinationDbId, 900, 0))}|{ChangeVector((sourceDbId, 100, 1))}";
            var local = $"{ChangeVector((destinationDbId, 1, 0))}|{ChangeVector((sourceDbId, 100, 1))}";
            var remoteWithAdvancedVersion = $"{ChangeVector((destinationDbId, 0, 0))}|{ChangeVector((sourceDbId, 104, 1))}";

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remoteWithAdvancedOrderOnly, local));
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remoteWithAdvancedVersion, local));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void GetConflictStatusShouldRespectExplicitOrderMode()
        {
            var destinationDbId = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();
            var context = new TestChangeVectorContext();

            var remote = context.GetChangeVector(
                $"{ChangeVector((destinationDbId, 900, 0))}|{ChangeVector((sourceDbId, 100, 1))}");
            var local = context.GetChangeVector(
                $"{ChangeVector((destinationDbId, 1, 0))}|{ChangeVector((sourceDbId, 100, 1))}");

            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote, local));
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local, mode: ChangeVectorMode.Order));
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ChangeVectorMergeShouldMergeCompositeOrderAndVersionSeparately()
        {
            var destinationDbId0 = Guid.NewGuid();
            var destinationDbId1 = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();
            var context = new TestChangeVectorContext();

            var left = context.GetChangeVector($"{ChangeVector((destinationDbId0, 500, 0))}|{ChangeVector((sourceDbId, 100, 2))}");
            var right = context.GetChangeVector($"{ChangeVector((destinationDbId1, 700, 1))}|{ChangeVector((sourceDbId, 95, 2))}");

            var result = Raven.Server.Utils.ChangeVector.Merge(left, right, context);

            Assert.Equal($"{ChangeVector((destinationDbId0, 500, 0), (destinationDbId1, 700, 1))}|{ChangeVector((sourceDbId, 100, 2))}", result.AsString());
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void ChangeVectorMergeShouldTreatMonoVectorAsBothOrderAndVersion()
        {
            var destinationDbId = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();
            var context = new TestChangeVectorContext();

            var composite = context.GetChangeVector($"{ChangeVector((destinationDbId, 500, 0))}|{ChangeVector((sourceDbId, 100, 1))}");
            var mono = context.GetChangeVector(ChangeVector((sourceDbId, 110, 1)));

            var result = Raven.Server.Utils.ChangeVector.Merge(composite, mono, context);

            Assert.Equal($"{ChangeVector((destinationDbId, 500, 0), (sourceDbId, 110, 1))}|{ChangeVector((sourceDbId, 110, 1))}", result.AsString());
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void DistanceShouldUseVersionPartForCompositeChangeVectors()
        {
            var destinationDbId = Guid.NewGuid();
            var sourceDbId = Guid.NewGuid();

            var left = $"{ChangeVector((destinationDbId, 999, 0))}|{ChangeVector((sourceDbId, 104, 1))}";
            var right = $"{ChangeVector((destinationDbId, 1, 0))}|{ChangeVector((sourceDbId, 100, 1))}";

            Assert.Equal(4, ChangeVectorUtils.Distance(left, right));
            Assert.Equal(-4, ChangeVectorUtils.Distance(right, left));
        }

        public string ChangeVector(params (Guid dbId, long etag, int nodeTag)[] changeVectorEntries)
        {
            return changeVectorEntries.Select(x => (ChangeVectorEntry)(x.dbId.AsChangeVectorDbId(), x.etag, x.nodeTag))
                                      .ToArray()
                                      .SerializeVector();
        }

        public static string DbId() //not strictly needed -> it is a shortcut
        {
            var dbId = Guid.NewGuid();
            return dbId.AsChangeVectorDbId();
        }

        private sealed class TestChangeVectorContext : IChangeVectorOperationContext
        {
            public ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false)
            {
                return new ChangeVector(changeVector, throwOnRecursion, this);
            }

            public ChangeVector GetChangeVector(string version, string order)
            {
                return new ChangeVector(
                    new ChangeVector(version, throwOnRecursion: true, this),
                    new ChangeVector(order, throwOnRecursion: true, this));
            }
        }
    }
}
