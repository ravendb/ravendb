//-----------------------------------------------------------------------
// <copyright file="SystemParameters.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;

    /// <summary>
    /// This class provides static properties to set and get
    /// global ESENT system parameters.
    /// </summary>
    public static partial class SystemParameters
    {
        /// <summary>
        /// Gets or sets the maximum size of the database page cache. The size
        /// is in database pages. If this parameter is left to its default value, then the
        /// maximum size of the cache will be set to the size of physical memory when JetInit
        /// is called.
        /// </summary>
        public static int CacheSizeMax
        {
            get
            {
                return GetIntegerParameter(JET_param.CacheSizeMax);
            }

            set
            {
                SetIntegerParameter(JET_param.CacheSizeMax, value);
            }
        }

        /// <summary>
        /// Gets or sets the size of the database cache in pages. By default the
        /// database cache will automatically tune its size, setting this property
        /// to a non-zero value will cause the cache to adjust itself to the target
        /// size.
        /// </summary>
        public static int CacheSize
        {
            get
            {
                return GetIntegerParameter(JET_param.CacheSize);
            }

            set
            {
                SetIntegerParameter(JET_param.CacheSize, value);
            }
        }

        /// <summary>
        /// Gets or sets the size of the database pages, in bytes.
        /// </summary>
        public static int DatabasePageSize
        {
            get
            {
                return GetIntegerParameter(JET_param.DatabasePageSize);
            }

            set
            {
                SetIntegerParameter(JET_param.DatabasePageSize, value);
            }
        }

        /// <summary>
        /// Gets or sets the minimum size of the database page cache, in database pages.
        /// </summary>
        public static int CacheSizeMin
        {
            get
            {
                return GetIntegerParameter(JET_param.CacheSizeMin);
            }

            set
            {
                SetIntegerParameter(JET_param.CacheSizeMin, value);
            }
        }

        /// <summary>
        /// Gets or sets the threshold at which the database page cache begins evicting pages from the
        /// cache to make room for pages that are not cached. When the number of page buffers in the cache
        /// drops below this threshold then a background process will be started to replenish that pool
        /// of available buffers. This threshold is always relative to the maximum cache size as set by
        /// JET_paramCacheSizeMax. This threshold must also always be less than the stop threshold as
        /// set by JET_paramStopFlushThreshold.
        /// <para>
        /// The distance height of the start threshold will determine the response time that the database
        ///  page cache must have to produce available buffers before the application needs them. A high
        /// start threshold will give the background process more time to react. However, a high start
        /// threshold implies a higher stop threshold and that will reduce the effective size of the
        /// database page cache.
        /// </para>
        /// </summary>
        public static int StartFlushThreshold
        {
            get
            {
                return GetIntegerParameter(JET_param.StartFlushThreshold);
            }

            set
            {
                SetIntegerParameter(JET_param.StartFlushThreshold, value);
            }
        }

        /// <summary>
        /// Gets or sets the threshold at which the database page cache ends evicting pages from the cache to make
        /// room for pages that are not cached. When the number of page buffers in the cache rises above
        /// this threshold then the background process that was started to replenish that pool of available
        /// buffers is stopped. This threshold is always relative to the maximum cache size as set by
        /// JET_paramCacheSizeMax. This threshold must also always be greater than the start threshold
        /// as set by JET_paramStartFlushThreshold.
        /// <para>
        /// The distance between the start threshold and the stop threshold affects the efficiency with
        /// which database pages are flushed by the background process. A larger gap will make it
        /// more likely that writes to neighboring pages may be combined. However, a high stop
        /// threshold will reduce the effective size of the database page cache.
        /// </para>
        /// </summary>
        public static int StopFlushThreshold
        {
            get
            {
                return GetIntegerParameter(JET_param.StopFlushThreshold);
            }

            set
            {
                SetIntegerParameter(JET_param.StopFlushThreshold, value);
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of instances that can be created.
        /// </summary>
        public static int MaxInstances
        {
            get
            {
                return GetIntegerParameter(JET_param.MaxInstances);
            }

            set
            {
                SetIntegerParameter(JET_param.MaxInstances, value);
            }
        }

        /// <summary>
        /// Gets or sets the detail level of eventlog messages that are emitted
        /// to the eventlog by the database engine. Higher numbers will result
        /// in more detailed eventlog messages.
        /// </summary>
        public static int EventLoggingLevel
        {
            get
            {
                return GetIntegerParameter(JET_param.EventLoggingLevel);
            }

            set
            {
                SetIntegerParameter(JET_param.EventLoggingLevel, value);
            }
        }

        /// <summary>
        /// Gets the maximum key size. This depends on the Esent version and database
        /// page size.
        /// </summary>
        public static int KeyMost
        {
            get
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    return GetIntegerParameter(VistaParam.KeyMost);
                }

                // All pre-Vista versions of Esent have 255 byte keys
                return 255; 
            }
        }

        /// <summary>
        /// Gets the maximum number of components in a sort or index key.
        /// </summary>
        public static int ColumnsKeyMost
        {
            get
            {
                return Api.Impl.Capabilities.ColumnsKeyMost;
            }
        }

        /// <summary>
        /// Gets the maximum size of a bookmark. <seealso cref="Api.JetGetBookmark"/>.
        /// </summary>
        public static int BookmarkMost
        {
            get
            {
                // This correctly returns 256 on pre-Vista systems
                return KeyMost + 1;
            }
        }

        /// <summary>
        /// Gets the lv chunks size. This depends on the database page size.
        /// </summary>
        public static int LVChunkSizeMost
        {
            get
            {
                if (EsentVersion.SupportsWindows7Features)
                {
                    return GetIntegerParameter(Windows7Param.LVChunkSizeMost);
                }

                // Can't retrieve the size directly, determine it from the database page size
                const int ColumnLvPageOverhead = 82;
                return GetIntegerParameter(JET_param.DatabasePageSize) - ColumnLvPageOverhead;
            }
        }

        /// <summary>
        /// Gets or sets a value specifying the default values for the
        /// entire set of system parameters. When this parameter is set to
        /// a specific configuration, all system parameter values are reset
        /// to their default values for that configuration. If the
        /// configuration is set for a specific instance then global system
        /// parameters will not be reset to their default values.
        /// Small Configuration (0): The database engine is optimized for memory use. 
        /// Legacy Configuration (1): The database engine has its traditional defaults.
        /// <para>
        /// Supported on Windows Vista and up. Ignored on Windows XP and
        /// Windows Server 2003.
        /// </para>
        /// </summary>
        public static int Configuration
        {
            get
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    return GetIntegerParameter(VistaParam.Configuration);
                }

                // return the legacy configuration value
                return 1;
            }

            set
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    SetIntegerParameter(VistaParam.Configuration, value);
                }
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the database engine accepts
        /// or rejects changes to a subset of the system parameters. This
        /// parameter is used in conjunction with <see cref="Configuration"/> to
        /// prevent some system parameters from being set away from the selected
        /// configuration's defaults.
        /// <para>
        /// Supported on Windows Vista and up. Ignored on Windows XP and
        /// Windows Server 2003.
        /// </para>
        /// </summary>
        public static bool EnableAdvanced
        {
            get
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    return GetBoolParameter(VistaParam.EnableAdvanced);
                }

                // older versions always allow advanced settings
                return true;
            }

            set
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    SetBoolParameter(VistaParam.EnableAdvanced, value);
                }
            }
        }

        /// <summary>
        /// Set a system parameter which is an integer.
        /// </summary>
        /// <param name="param">The parameter to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetIntegerParameter(JET_param param, int value)
        {
            Api.JetSetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, param, value, null);
        }

        /// <summary>
        /// Get a system parameter which is an integer.
        /// </summary>
        /// <param name="param">The parameter to get.</param>
        /// <returns>The value of the parameter.</returns>
        private static int GetIntegerParameter(JET_param param)
        {
            int value = 0;
            string ignored;
            Api.JetGetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, param, ref value, out ignored, 0);
            return value;
        }

        /// <summary>
        /// Set a system parameter which is a boolean.
        /// </summary>
        /// <param name="param">The parameter to set.</param>
        /// <param name="value">The value to set.</param>
        private static void SetBoolParameter(JET_param param, bool value)
        {
            int setting = value ? 1 : 0;
            Api.JetSetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, param, setting, null);
        }

        /// <summary>
        /// Get a system parameter which is a boolean.
        /// </summary>
        /// <param name="param">The parameter to get.</param>
        /// <returns>The value of the parameter.</returns>
        private static bool GetBoolParameter(JET_param param)
        {
            int value = 0;
            string ignored;
            Api.JetGetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, param, ref value, out ignored, 0);
            return value != 0;
        }
    }
}