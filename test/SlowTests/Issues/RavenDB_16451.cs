using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16451 : RavenTestBase
    {
        public RavenDB_16451(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void LoadDocumentTest()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new CoursesByCourseLesson());
                using (var session = store.OpenSession())
                {
                    var lesson1 = new Lesson();
                    session.Store(lesson1);
                    var lesson2 = new Lesson();
                    session.Store(lesson2);
                    var courseLesson1 = new CourseLesson { LessonId = lesson1.Id };
                    var courseLesson2 = new CourseLesson { LessonId = lesson2.Id };

                    var course = new Course();
                    course.Lessons.Add(courseLesson1);
                    course.Lessons.Add(courseLesson2);

                    session.Store(course);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<CourseLessonProjection, CoursesByCourseLesson>()
                        .Where(x => x.CourseId == "courses/1-A")
                        .ProjectInto<CourseLessonProjection>()
                        .ToList();

                    Assert.Equal(2, query.Count);
                    Assert.Equal(2, query.Count(x => x.CourseId == "courses/1-A"));
                    Assert.Equal(1, query.Count(x => x.LessonId == "lessons/1-A"));
                    Assert.Equal(1, query.Count(x => x.LessonId == "lessons/2-A"));
                    Assert.Equal(1, query.Count(x => x.Index == 0));
                    Assert.Equal(1, query.Count(x => x.Index == 1));
                }
            }
        }

        private class CoursesByCourseLesson : AbstractIndexCreationTask<Course, CourseLessonProjection>
        {
            public override string IndexName => "Courses/ByCourseLesson";

            public CoursesByCourseLesson()
            {
                Map = courses =>
                    from course in courses
                    from index in Enumerable.Range(0, course.Lessons.Count)
                    let lesson = LoadDocument<Lesson>(course.Lessons[index].LessonId)
                    select new CourseLessonProjection()
                    {
                        CourseId = course.Id,
                        Index = index,
                        LessonId = lesson.Id,
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Course
        {
            public string Id { get; set; }

            public List<CourseLesson> Lessons { get; } = new List<CourseLesson>();
        }

        private class Lesson
        {
            public string Id { get; set; }
        }

        private class CourseLesson
        {
            public string LessonId { get; set; }
        }

        private class CourseLessonProjection
        {
            public string CourseId { get; set; }

            public int Index { get; set; }

            public string LessonId { get; set; }
        }
    }
}
