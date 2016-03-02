using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Metrics;
using Metrics.Core;
using Metrics.Utils;

namespace Test
{
    class Program
    {
        private const int MetricsInstances = 100;

        public class SingleMetricsInstance
        {
            private readonly MetricsContext _context;
            private readonly int _id;
            private MeterMetric metric;
            private PerSecondMetric perSecondMetric;
            private BufferedAverageMeter bufferedAverageMetric;
            private HistogramMetric histogram;

            public SingleMetricsInstance(MetricsContext context, int id)
            {
                _context = context;
                _id = id;
                metric = (MeterMetric)context.Meter("simpleMetric"+id);
                perSecondMetric = (PerSecondMetric)context.PerSecondMetric("perSecondMetric"+id);
                bufferedAverageMetric = (BufferedAverageMeter)context.BufferedAverageMeter("bufferedMetric"+id, 10, 1);
                histogram = (HistogramMetric)context.Histogram("histogram"+id);
            }

            public void MarkAll(int val) 
            {
                metric.Mark();
                perSecondMetric.Mark();
                bufferedAverageMetric.Mark();
                histogram.Update(val);
            }

            public double[] Values()
            {
                return new[]
                {
                    metric.Value.MeanRate,
                    metric.Value.OneMinuteRate,
                    metric.Value.FiveMinuteRate,
                    metric.Value.FifteenMinuteRate,
                    perSecondMetric.Value,
                    bufferedAverageMetric.Value,
                    histogram.Value.Mean
                };
            }

            
        }
        static void Main(string[] args)
        {
            var context = Metric.Context("ContextName");
            var metricsInstances = new SingleMetricsInstance[MetricsInstances];
            for (var i = 0; i < MetricsInstances; i++)
                metricsInstances[i] = new SingleMetricsInstance(context, i);

            int TasksAmount=50;
            Task[] tasks = new Task[TasksAmount];
            CancellationTokenSource tcs = new CancellationTokenSource();
            
            for (var i=0;i<TasksAmount; i++)
            {
                
                tasks[i] = Task.Run((Action) (()=>MarkTask(metricsInstances)),tcs.Token);
            }
            Task.Run(() => PrintData(metricsInstances, tcs.Token));
            Task.Run(() => GetInput(tcs));
            while(tcs.IsCancellationRequested==false)
                Thread.Sleep(100);
            
        }

        private static void GetInput(CancellationTokenSource cts)
        {
            while (true)
            {
                if (Console.ReadKey().Key==ConsoleKey.Q)
                    cts.Cancel();
                else
                    continue;
                
            }
        }
        private static void PrintData(SingleMetricsInstance[] instances, CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                Console.WriteLine(string.Join(" ; ", 
                instances.Select(x => x.Values()
                    ).Aggregate((x, u) =>
                    {
                        var z = new double[x.Length];
                        for (var i = 0; i < x.Length; i++)
                            z[i] = (x[i] + u[i])/2;
                        return z;
                    }).Select(x=>x.ToString())));
                Thread.Sleep(1000);
            }
        }

        private static void MarkTask(SingleMetricsInstance[] metricsInstances)
        {
            var rand = new Random();
            for (var j = 0; j < 20 * 60; j++)
            {
                var cycleStartNano = Clock.Nanoseconds;

                for (var i = 0; i < (j % 2 == 0 ? 25 : 50); i++)
                {
                    foreach (var singleMetricsInstance in metricsInstances)
                    {
                        singleMetricsInstance.MarkAll(i);
                    }
                }

                var millisecondsTimeout = 1000 - (Clock.Nanoseconds - cycleStartNano) / Clock.NANOSECONDS_IN_MILISECOND;
                Thread.Sleep((int)millisecondsTimeout);
            }
        }
    }
}
