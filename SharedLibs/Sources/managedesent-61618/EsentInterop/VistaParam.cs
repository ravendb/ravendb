//-----------------------------------------------------------------------
// <copyright file="VistaParam.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Vista
{
    /// <summary>
    /// System parameters that have been added to the Vista version of ESENT.
    /// </summary>
    public static class VistaParam
    {
        /// <summary>
        /// This parameter controls the number of B+ Tree resources cached by
        /// the instance after the tables they represent have been closed by
        /// the application. Large values for this parameter will cause the
        /// database engine to use more memory but will increase the speed
        /// with which a large number of tables can be opened randomly by
        /// the application. This is useful for applications that have a
        /// schema with a very large number of tables.
        /// </summary>
        public const JET_param CachedClosedTables = (JET_param)125;

        /// <summary>
        /// This parameter exposes multiple sets of default values for the
        /// entire set of system parameters. When this parameter is set to
        /// a specific configuration, all system parameter values are reset
        /// to their default values for that configuration. If the
        /// configuration is set for a specific instance then global system
        /// parameters will not be reset to their default values.
        /// Small Configuration (0): The database engine is optimized for memory use. 
        /// Legacy Configuration (1): The database engine has its traditional defaults.
        /// </summary>
        public const JET_param Configuration = (JET_param)129;

        /// <summary>
        /// This parameter is used to control when the database engine accepts
        /// or rejects changes to a subset of the system parameters. This
        /// parameter is used in conjunction with <see cref="Configuration"/> to
        /// prevent some system parameters from being set away from the selected
        /// configuration's defaults.
        /// </summary>
        public const JET_param EnableAdvanced = (JET_param)130;

        /// <summary>
        /// This read-only parameter indicates the maximum allowable index key
        /// length that can be selected for the current database page size
        /// (as configured by <see cref="JET_param.DatabasePageSize"/>).
        /// </summary>
        public const JET_param KeyMost = (JET_param)134;

        /// <summary>
        /// Set the name associated with table class 1.
        /// </summary>
        public const JET_param TableClass1Name = (JET_param)137;

        /// <summary>
        /// Set the name associated with table class 2.
        /// </summary>
        public const JET_param TableClass2Name = (JET_param)138;

        /// <summary>
        /// Set the name associated with table class 3.
        /// </summary>
        public const JET_param TableClass3Name = (JET_param)139;

        /// <summary>
        /// Set the name associated with table class 4.
        /// </summary>
        public const JET_param TableClass4Name = (JET_param)140;

        /// <summary>
        /// Set the name associated with table class 5.
        /// </summary>
        public const JET_param TableClass5Name = (JET_param)141;

        /// <summary>
        /// Set the name associated with table class 6.
        /// </summary>
        public const JET_param TableClass6Name = (JET_param)142;

        /// <summary>
        /// Set the name associated with table class 7.
        /// </summary>
        public const JET_param TableClass7Name = (JET_param)143;

        /// <summary>
        /// Set the name associated with table class 8.
        /// </summary>
        public const JET_param TableClass8Name = (JET_param)144;

        /// <summary>
        /// Set the name associated with table class 9.
        /// </summary>
        public const JET_param TableClass9Name = (JET_param)145;

        /// <summary>
        /// Set the name associated with table class 10.
        /// </summary>
        public const JET_param TableClass10Name = (JET_param)146;

        /// <summary>
        /// Set the name associated with table class 11.
        /// </summary>
        public const JET_param TableClass11Name = (JET_param)147;

        /// <summary>
        /// Set the name associated with table class 12.
        /// </summary>
        public const JET_param TableClass12Name = (JET_param)148;

        /// <summary>
        /// Set the name associated with table class 13.
        /// </summary>
        public const JET_param TableClass13Name = (JET_param)149;

        /// <summary>
        /// Set the name associated with table class 14.
        /// </summary>
        public const JET_param TableClass14Name = (JET_param)150;

        /// <summary>
        /// Set the name associated with table class 15.
        /// </summary>
        public const JET_param TableClass15Name = (JET_param)151;
    }
}