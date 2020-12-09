using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Replication;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class ChangeVectorConflictStatusTests : NoDisposalNeeded
    {
        public ChangeVectorConflictStatusTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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

        [Fact]
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

        [Fact]
        public void Two_empty_ChangeVectors()
        {
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, string.Empty));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(null, string.Empty));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, null));
        }

        [Fact]
        public void Empty_remote_change_vector_should_generate_already_merged()
        {
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(string.Empty, ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2))));
        }

        [Fact]
        public void Empty_local_change_vector()
        {
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2)), string.Empty));
        }

        [Fact]
        public void Change_vector_has_negative_etag()
        {
            var changeVectorWithNegatoveEtag = ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), -3, 2));
            var changeVector = ChangeVector((Guid.NewGuid(), 2, 1), (Guid.NewGuid(), 3, 2));

            Assert.Throws<ArgumentException>(() =>
                ChangeVectorUtils.GetConflictStatus(changeVectorWithNegatoveEtag, changeVector));

            Assert.Throws<ArgumentException>(() =>
                ChangeVectorUtils.GetConflictStatus(changeVector, changeVectorWithNegatoveEtag));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_all_remote_etags_large_than_local()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]), (dbIds[2], 5, tags[2]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_all_remote_etags_large_than_local()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 5, tags[2]), (dbIds[1], 5, tags[1]), (dbIds[0], 5, tags[0]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_all_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_all_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 1, tags[2]), (dbIds[1], 1, tags[1]), (dbIds[0], 1, tags[0]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 5, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_same_order_and_some_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 5, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Remote_has_entries_not_in_local_with_entries_not_same_order_and_some_local_etags_large_than_remote()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

            var tags = Enumerable.Range(0, 3).ToArray();
            var remote = ChangeVector((dbIds[2], 1, tags[2]), (dbIds[1], 5, tags[1]), (dbIds[0], 1, tags[0]));
            var local = ChangeVector((dbIds[0], 5, tags[0]), (dbIds[1], 1, tags[1]));

            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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


        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Theory]
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

        [Fact]
        public void Different_change_vectors_with_different_prefix_remote_smaller_with_remote_etags_smaller()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[0], 10, tags[0]), (dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            Assert.Equal(ConflictStatus.AlreadyMerged, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Different_change_vectors_with_different_prefix_local_smaller_with_remote_etags_smaller()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            var local = ChangeVector((dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Different_change_vectors_with_different_prefix_remote_smaller_with_remote_etags_larger()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            var local = ChangeVector((dbIds[0], 1, tags[0]), (dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            Assert.Equal(ConflictStatus.Conflict, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
        public void Different_change_vectors_with_different_prefix_local_smaller_with_remote_etags_larger()
        {
            var dbIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var tags = Enumerable.Range(0, 3).ToArray();

            var remote = ChangeVector((dbIds[0], 10, tags[0]), (dbIds[1], 10, tags[1]), (dbIds[2], 10, tags[2]));
            var local = ChangeVector((dbIds[1], 1, tags[1]), (dbIds[2], 1, tags[2]));
            Assert.Equal(ConflictStatus.Update, ChangeVectorUtils.GetConflictStatus(remote, local));
        }

        [Fact]
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
    }
}
