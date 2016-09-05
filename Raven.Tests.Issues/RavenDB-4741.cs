// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4741.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;
using Raven.Tests.Common;
using RTP = Raven.Database.Impl.BackgroundTaskExecuter.RavenThreadPool;

namespace Raven.Tests.Issues
{
    public class TP_Issues : RavenTest
    {
        [Fact]
        public void one_task_one_failing()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 1).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (int input) =>
                    {
                        const int number = 100;
                        var result = number / input;
                    },database:null);
                });
            }
        }

        [Fact]
        public void one_task_one_failing_with_enumerator()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 1).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                    {
                        while (input.MoveNext())
                        {
                            const int number = 100;
                            var result = number / input.Current;
                        }
                    },database:null);
                });
            }
        }

        [Fact]
        public void three_tasks_one_failing()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 3).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (int input) =>
                    {
                        const int number = 100;
                        var result = number / input;
                    },database:null);
                });
            }
        }

        [Fact]
        public void three_tasks_one_failing_with_enumerator()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 10000).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                    {
                        while (input.MoveNext())
                        {
                            const int number = 100;
                            var result = number / input.Current;
                        }
                    },database:null);
                });
            }
        }

        [Fact]
        public void three_tasks_one_failing_with_allowPartialBatchResumption()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 3).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (int input) =>
                    {
                        const int number = 100;
                        var result = number / input;

                        if (input == 1)
                            Thread.Sleep(3000);

                    }, allowPartialBatchResumption: true,database:null);
                });
            }
        }

        [Fact]
        public void three_tasks_one_failing_with_allowPartialBatchResumption_with_enumerator()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 10000).ToList();

                Assert.Throws(typeof(DivideByZeroException), () =>
                {
                    tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                    {
                        while (input.MoveNext())
                        {
                            const int number = 100;
                            var result = number / input.Current;

                            if (input.Current == 1 || input.Current == 5000)
                                Thread.Sleep(1000);
                        }
                    },database:null);
                });
            }
        }

        [Fact]
        public void three_tasks_one_failing_with_allowPartialBatchResumption_shouldnt_throw()
        {
            using (var tp = new RTP(8))
            {
                tp.Start();
                var range = Enumerable.Range(0, 3).ToList();
                Assert.DoesNotThrow(() =>
                {
                    tp.ExecuteBatch(range, (int input) =>
                    {
                        const int number = 100;
                        if (input == 0)
                            Thread.Sleep(30000);

                        var result = number / input;
                    }, allowPartialBatchResumption: true, database: null);
                });
            }
        }
    }
}
