namespace Raven.NewClient.Abstractions.Counters
{
    public class CountersBatchOptions
    {
        public CountersBatchOptions()
        {
            BatchSizeLimit = 512;
            BatchReadTimeoutInMilliseconds = 5000;
            ConnectionReopenTimingInMilliseconds = 110000; //default for IIS is 2 minutes
            StreamingInitializeTimeout = 10000;
        }

        /// <summary>
        /// Number of counter changes to send in each batch.
        /// <para>Value:</para>
        /// <para>512 by default</para>
        /// </summary>
        /// <value>512 by default</value>
        public int BatchSizeLimit { get; set; }

        /// <summary>
        /// Maximum timeout in milliseconds to wait counter change writes. Exception will be thrown when timeout is elapsed.
        /// <para>Value:</para>
        /// <para>5000 milliseconds by default</para>
        /// </summary>
        /// <value>5000 milliseconds by default</value>
        public int BatchReadTimeoutInMilliseconds { get; set; }

        /// <summary>
        /// in order to bypass IIS limitations on inactive connection timeouts, close and reopen stream after this time
        /// </summary>
        public int ConnectionReopenTimingInMilliseconds { get; set; }

        /// <summary>
        /// Maximum timeout in milliseconds to wait until streaming is initialized. Exception will be thrown when timeout is elapsed.
        /// <para>Value:</para>
        /// <para>10000 milliseconds by default</para>
        /// </summary>
        /// <value>10000 milliseconds by default</value>
        public int StreamingInitializeTimeout { get; set; }
    }
}
