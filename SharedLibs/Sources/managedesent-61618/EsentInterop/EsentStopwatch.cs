//-----------------------------------------------------------------------
// <copyright file="EsentStopwatch.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// Provides a set of methods and properties that you can use to measure
    /// ESENT work statistics for a thread. If the current version of ESENT
    /// doesn't support <see cref="VistaApi.JetGetThreadStats"/> then all 
    /// ESENT statistics will be 0.
    /// </summary>
    public class EsentStopwatch
    {
        /// <summary>
        /// Used to measure how long statistics are collected for.
        /// </summary>
        private Stopwatch stopwatch;

        /// <summary>
        /// The stats at the start of our collection.
        /// </summary>
        private JET_THREADSTATS statsAtStart;

        /// <summary>
        /// Gets a value indicating whether the EsentStopwatch timer is running. 
        /// </summary>
        public bool IsRunning { get; private set; }

        /// <summary>
        /// Gets the total ESENT work stats measured by the current instance.
        /// </summary>
        public JET_THREADSTATS ThreadStats { get; private set; }

        /// <summary>
        /// Gets the total elapsed time measured by the current instance.
        /// </summary>
        public TimeSpan Elapsed { get; private set; }

        /// <summary>
        /// Initializes a new EsentStopwatch instance and starts
        /// measuring elapsed time. 
        /// </summary>
        /// <returns>A new, running EsentStopwatch.</returns>
        public static EsentStopwatch StartNew()
        {
            var stopwatch = new EsentStopwatch();
            stopwatch.Start();
            return stopwatch;
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Stopwatch"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Stopwatch"/>.
        /// </returns>
        public override string ToString()
        {
            return this.IsRunning ? "EsentStopwatch (running)" : this.Elapsed.ToString();
        }

        /// <summary>
        /// Starts measuring ESENT work.
        /// </summary>
        public void Start()
        {
            this.Reset();
            this.stopwatch = Stopwatch.StartNew();
            this.IsRunning = true;
            if (EsentVersion.SupportsVistaFeatures)
            {
                VistaApi.JetGetThreadStats(out this.statsAtStart);
            }
        }

        /// <summary>
        /// Stops measuring ESENT work.
        /// </summary>
        public void Stop()
        {
            if (this.IsRunning)
            {
                this.IsRunning = false;
                this.stopwatch.Stop();
                this.Elapsed = this.stopwatch.Elapsed;
                if (EsentVersion.SupportsVistaFeatures)
                {
                    JET_THREADSTATS statsAtEnd;
                    VistaApi.JetGetThreadStats(out statsAtEnd);
                    this.ThreadStats = statsAtEnd - this.statsAtStart;
                }
            }
        }

        /// <summary>
        /// Stops time interval measurement and resets the thread statistics.
        /// </summary>
        public void Reset()
        {
            this.stopwatch = null;
            this.ThreadStats = new JET_THREADSTATS();
            this.Elapsed = TimeSpan.Zero;
            this.IsRunning = false;
        }
    }
}