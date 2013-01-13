//-----------------------------------------------------------------------
// <copyright file="Dependencies.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Practices.Unity;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Dependency Injection Container
    /// </summary>
    public static class Dependencies
    {
        /// <summary>
        /// Stores global configuration and object creation functions. This provides 
        /// Inversion of Control (IoC) -- objects and configuration are looked up from the
        /// container so they can be changed without changing the client classes.
        /// </summary>
        private static IUnityContainer container;

        /// <summary>
        /// Initializes static members of the Dependencies class.
        /// </summary>
        static Dependencies()
        {
            InitializeContainer();
        }

        /// <summary>
        /// Gets the global object container.
        /// </summary>
        internal static IUnityContainer Container
        {
            get
            {
                return container;
            }
        }

        /// <summary>
        /// Initialize the object container.
        /// </summary>
        internal static void InitializeContainer()
        {
            container = new UnityContainer()
                .RegisterType<SqlConnection, Sql.Parsing.Parser>()
                .RegisterType<ISqlImpl, SqlImplBase>()
                .RegisterType<IConnectionManager, ConnectionManager>(new ContainerControlledLifetimeManager());  // singleton

            // The Unicode Encoding will throw an exception on an invalid string
            Encoding textEncoding = new UnicodeEncoding(false, false, true);
            Encoding asciiTextEncoding = new ASCIIEncoding();
            Container.RegisterType<DataConversion>(
                new ContainerControlledLifetimeManager(),   // singleton
                new InjectionConstructor(textEncoding, asciiTextEncoding));

            // Configuration
            RegisterDefaultParameters();
            RegisterColtypMappings();
        }

        /// <summary>
        /// Register the default Jet configuration.
        /// </summary>
        private static void RegisterDefaultParameters()
        {
            var globalParameters = new List<JetParameter>();

            if (EsentVersion.SupportsVistaFeatures)
            {
                globalParameters.AddRange(new List<JetParameter>
                {
                    new JetParameter(VistaParam.Configuration, 0),
                    new JetParameter(VistaParam.EnableAdvanced, 1),
                    new JetParameter(JET_param.CacheSizeMin, 64),
                    new JetParameter(JET_param.CacheSizeMax, int.MaxValue),
                });
            }

            if (EsentVersion.SupportsWindows7Features)
            {
                globalParameters.AddRange(new List<JetParameter>
                {
                    new JetParameter(Windows7Param.WaypointLatency, 1),
                });
            }

            // The Vista advanced parameters reset a lot of defaults, so set these
            // overrides afterwards
            globalParameters.AddRange(new List<JetParameter>
            {
                new JetParameter(JET_param.DatabasePageSize, 8192),
            });
            Container.RegisterInstance<IEnumerable<JetParameter>>("GlobalParameters", globalParameters.AsReadOnly());

            var defaultParameters = new List<JetParameter>
            {
                new JetParameter(JET_param.CircularLog, 1),
                new JetParameter(JET_param.CreatePathIfNotExist, 1),
                new JetParameter(JET_param.NoInformationEvent, 1),
                new JetParameter(JET_param.MaxVerPages, 64 * 1024),
                new JetParameter(JET_param.MaxOpenTables, 1024),
                new JetParameter(JET_param.LogFileSize, 256),
            };
            Container.RegisterInstance<IEnumerable<JetParameter>>("DefaultInstanceParameters", defaultParameters.AsReadOnly());
        }

        /// <summary>
        /// Initializes the object registry with column type mappings.
        /// </summary>
        private static void RegisterColtypMappings()
        {
            var columntypeToJetColtyp = new Dictionary<ColumnType, JET_coltyp>
            {
                { ColumnType.Binary, JET_coltyp.LongBinary },
                { ColumnType.Bool, JET_coltyp.Bit },
                { ColumnType.Byte, JET_coltyp.UnsignedByte },
                { ColumnType.DateTime, JET_coltyp.DateTime },
                { ColumnType.Double, JET_coltyp.IEEEDouble },
                { ColumnType.Float, JET_coltyp.IEEESingle },
                { ColumnType.Guid, VistaColtyp.GUID },
                { ColumnType.Int32, JET_coltyp.Long },
                { ColumnType.Int64, JET_coltyp.Currency },
                { ColumnType.Int16, JET_coltyp.Short },
                { ColumnType.Text, JET_coltyp.LongText },
                { ColumnType.AsciiText, JET_coltyp.LongText },
                { ColumnType.UInt32, VistaColtyp.UnsignedLong },
                { ColumnType.UInt16, VistaColtyp.UnsignedShort },
            };
            Container.RegisterInstance<Dictionary<ColumnType, JET_coltyp>>(columntypeToJetColtyp);

            // This isn't an inversion of the above table becuase of some aliased coltyps
            var jetcoltypToColumnType = new Dictionary<JET_coltyp, ColumnType>
            {
                { JET_coltyp.Binary, ColumnType.Binary },
                { JET_coltyp.Bit, ColumnType.Bool },
                { JET_coltyp.Currency, ColumnType.Int64 },
                { JET_coltyp.DateTime, ColumnType.DateTime },
                { JET_coltyp.IEEEDouble, ColumnType.Double },
                { JET_coltyp.IEEESingle, ColumnType.Float },
                { JET_coltyp.Long, ColumnType.Int32 },
                { JET_coltyp.LongBinary, ColumnType.Binary },
                { JET_coltyp.Short, ColumnType.Int16 },
                { JET_coltyp.UnsignedByte, ColumnType.Byte },
                { VistaColtyp.GUID, ColumnType.Guid },
                { VistaColtyp.LongLong, ColumnType.Int64 },
                { VistaColtyp.UnsignedLong, ColumnType.UInt32 },
                { VistaColtyp.UnsignedShort, ColumnType.UInt16 },
            };
            Container.RegisterInstance<Dictionary<JET_coltyp, ColumnType>>(jetcoltypToColumnType);
        }
    }
}
