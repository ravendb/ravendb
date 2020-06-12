using System;
using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15133 : RavenTestBase
    {
        public RavenDB_15133(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void Can_Support_Guid_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var foodId = Guid.NewGuid();

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                Id = foodId
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.Id == foodId));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_To_String_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var foodId = Guid.NewGuid();

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                Id = foodId
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.Id.ToString() == foodId.ToString()));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_Enum_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                const Score score = Score.Five;

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                Score = score
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.Score == score));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_DateTime_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var dateTime = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                DateTime = dateTime
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.DateTime == dateTime));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_DateTimeOffset_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var dateTimeOffset = DateTimeOffset.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                DateTimeOffset = dateTimeOffset
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.DateTimeOffset == dateTimeOffset));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_TimeSpan_In_Patch_Filter()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var timeSpan = TimeSpan.FromDays(5);

                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        Foods = new List<DiaryFood>
                        {
                            new DiaryFood
                            {
                                TimeSpan = timeSpan
                            }
                        }
                    }, docId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DiaryFood>(docId,
                        x => x.Foods,
                        foods => foods.RemoveAll(food => food.TimeSpan == timeSpan));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Empty(diary.Foods);
                }
            }
        }

        [Fact]
        public void Can_Support_DateTime_In_Patch()
        {
            using (var store = GetDocumentStore())
            {
                const string docId = "diaries/1";
                var now = DateTime.Now;
                using (var session = store.OpenSession())
                {
                    session.Store(new Diary
                    {
                        DateTime = now
                    }, docId);

                    session.SaveChanges();
                }

                var utcNow = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Diary, DateTime>(docId, x => x.DateTime, utcNow);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var diary = session.Load<Diary>(docId);
                    Assert.Equal(utcNow, diary.DateTime);
                }
            }
        }

        public class Diary
        {
            public Diary()
            {
                Foods = new List<DiaryFood>();

            }

            public List<DiaryFood> Foods { get; set; }

            public DateTime DateTime { get; set; }
        }

        public class DiaryFood
        {
            public Guid Id { get; set; }

            public Score Score { get; set; }

            public DateTime DateTime { get; set; }

            public DateTimeOffset DateTimeOffset { get; set; }

            public TimeSpan TimeSpan { get; set; }
        }

        public enum Score
        {
            One,
            Two,
            Three,
            Four,
            Five
        }
    }
}
