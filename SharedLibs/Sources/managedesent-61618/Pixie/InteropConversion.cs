//-----------------------------------------------------------------------
// <copyright file="InteropConversion.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Practices.Unity;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Convert objects to and from Esent Interop objects.
    /// </summary>
    internal class InteropConversion
    {
        /// <summary>
        /// A mapping of Jet column types to column types.
        /// </summary>
        private readonly Dictionary<JET_coltyp, ColumnType> coltypToColumnTypeMapping;

        /// <summary>
        /// A mapping of column types to Jet column types.
        /// </summary>
        private readonly Dictionary<ColumnType, JET_coltyp> columnTypeToColtypMapping;

        /// <summary>
        /// Initializes a new instance of the InteropConversion class.
        /// </summary>
        /// <param name="coltypToColumnTypeMapping">A mapping of Jet column types to ColumnTypes.</param>
        /// <param name="columnTypeToColtypMapping">A mapping of ColumnTypes to Jet column types.</param>
        [InjectionConstructor]
        public InteropConversion(
            Dictionary<JET_coltyp, ColumnType> coltypToColumnTypeMapping,
            Dictionary<ColumnType, JET_coltyp> columnTypeToColtypMapping)
        {
            this.coltypToColumnTypeMapping = coltypToColumnTypeMapping;
            this.columnTypeToColtypMapping = columnTypeToColtypMapping;
        }

        /// <summary>
        /// Create a ColumnMetaData from an Interop.ColumnInfo
        /// </summary>
        /// <param name="info">The columninfo.</param>
        /// <returns>A ColumnMetaData created from the ColumnInfo</returns>
        public ColumnMetaData CreateColumnMetaDataFromColumnInfo(ColumnInfo info)
        {
            var metadata = new ColumnMetaData
            {
                Name = info.Name,
                Type = this.ColumnTypeFromColumnInfo(info),
                IsAutoincrement =
                   IsColumndefOptionSet(info.Grbit, ColumndefGrbit.ColumnAutoincrement),
                IsNotNull = IsColumndefOptionSet(info.Grbit, ColumndefGrbit.ColumnNotNULL),
                IsVersion = IsColumndefOptionSet(info.Grbit, ColumndefGrbit.ColumnVersion),
                IsEscrowUpdate =
                   IsColumndefOptionSet(info.Grbit, ColumndefGrbit.ColumnEscrowUpdate),
                MaxSize = info.MaxLength,
                DefaultValue = info.DefaultValue,
                Columnid = info.Columnid
            };
            return metadata;
        }

        /// <summary>
        /// Create a JET_COLUMNDEF from a ColumnDefintion.
        /// </summary>
        /// <param name="definition">The column definition to convert.</param>
        /// <returns>A JET_COLUMNDEF representing the ColumnDefintion.</returns>
        public JET_COLUMNDEF CreateColumndefFromColumnDefinition(ColumnDefinition definition)
        {
            ColumndefGrbit grbit = CalculateColumndefGrbit(definition);

            var columndef = new JET_COLUMNDEF
            {
                cbMax = definition.MaxSize,
                coltyp = this.columnTypeToColtypMapping[definition.Type],
                cp = (ColumnType.AsciiText == definition.Type) ? JET_CP.ASCII : JET_CP.Unicode,
                grbit = grbit,
            };

            return columndef;
        }

        /// <summary>
        /// Determine the ColumndefGrbit for the column definition.
        /// </summary>
        /// <param name="definition">The column definition.</param>
        /// <returns>The grbit to use when creating the column.</returns>
        private static ColumndefGrbit CalculateColumndefGrbit(ColumnDefinition definition)
        {
            ColumndefGrbit grbit = ColumndefGrbit.None;
            if (definition.IsAutoincrement)
            {
                grbit |= ColumndefGrbit.ColumnAutoincrement;
            }

            if (definition.IsNotNull)
            {
                grbit |= ColumndefGrbit.ColumnNotNULL;
            }

            if (definition.IsVersion)
            {
                grbit |= ColumndefGrbit.ColumnVersion;
            }

            if (EsentVersion.SupportsWindows7Features)
            {
                // Only long-value columns can be compressed
                if (ColumnType.Binary == definition.Type
                    || ColumnType.AsciiText == definition.Type
                    || ColumnType.Text == definition.Type)
                {
                    grbit |= Windows7Grbits.ColumnCompressed;
                }
            }

            return grbit;
        }

        /// <summary>
        /// Determine if the given ColumndefGrbit option is set in the grbit.
        /// </summary>
        /// <param name="grbit">The grbit to look at.</param>
        /// <param name="option">The option to check for.</param>
        /// <returns>True if the option is set, false otherwise.</returns>
        private static bool IsColumndefOptionSet(ColumndefGrbit grbit, ColumndefGrbit option)
        {
            return option == (grbit & option);
        }

        /// <summary>
        /// Return the column type of the given column info.
        /// </summary>
        /// <param name="info">The column info object.</param>
        /// <returns>The Esent column type that the Esent column has.</returns>
        private ColumnType ColumnTypeFromColumnInfo(ColumnInfo info)
        {
            if (JET_coltyp.Text == info.Coltyp || JET_coltyp.LongText == info.Coltyp)
            {
                if (JET_CP.ASCII == info.Cp)
                {
                    return ColumnType.AsciiText;
                }

                return ColumnType.Text;
            }

            return this.coltypToColumnTypeMapping[info.Coltyp];
        }
    }
}