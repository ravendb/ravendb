/*
    Copyright (c) 2002-2021, Npgsql

    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.

    IN NO EVENT SHALL NPGSQL BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
    SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
    ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
    Npgsql HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    NPGSQL SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
    PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Npgsql
    HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
    OR MODIFICATIONS.
 */

namespace Raven.Server.Integrations.PostgreSQL
{
    /// <summary>
    /// Provides constants for PostgreSQL error codes.
    /// </summary>
    /// <remarks>
    /// See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
    /// </remarks>
    public static class PgErrorCodes
    {
        #region Class 00 - Successful Completion

        public const string SuccessfulCompletion = "00000";

        #endregion Class 00 - Successful Completion

        #region Class 01 - Warning

        public const string Warning = "01000";
        public const string DynamicResultSetsReturnedWarning = "0100C";
        public const string ImplicitZeroBitPaddingWarning = "01008";
        public const string NullValueEliminatedInSetFunctionWarning = "01003";
        public const string PrivilegeNotGrantedWarning = "01007";
        public const string PrivilegeNotRevokedWarning = "01006";
        public const string StringDataRightTruncationWarning = "01004";
        public const string DeprecatedFeatureWarning = "01P01";

        #endregion Class 01 - Warning

        #region Class 02 - No Data

        public const string NoData = "02000";
        public const string NoAdditionalDynamicResultSetsReturned = "02001";

        #endregion Class 02 - No Data

        #region Class 03 - SQL Statement Not Yet Complete

        public const string SqlStatementNotYetComplete = "03000";

        #endregion Class 03 - SQL Statement Not Yet Complete

        #region Class 08 - Connection Exception

        public const string ConnectionException = "08000";
        public const string ConnectionDoesNotExist = "08003";
        public const string ConnectionFailure = "08006";
        public const string SqlClientUnableToEstablishSqlConnection = "08001";
        public const string SqlServerRejectedEstablishmentOfSqlConnection = "08004";
        public const string TransactionResolutionUnknown = "08007";
        public const string ProtocolViolation = "08P01";

        #endregion Class 08 - Connection Exception

        #region Class 09 - Triggered Action Exception

        public const string TriggeredActionException = "09000";

        #endregion Class 09 - Triggered Action Exception

        #region Class 0A - Feature Not Supported

        public const string FeatureNotSupported = "0A000";

        #endregion Class 0A - Feature Not Supported

        #region Class 0B - Invalid Transaction Initiation

        public const string InvalidTransactionInitiation = "0B000";

        #endregion Class 0B - Invalid Transaction Initiation

        #region Class 0F - Locator Exception

        public const string LocatorException = "0F000";
        public const string InvalidLocatorSpecification = "0F001";

        #endregion Class 0F - Locator Exception

        #region Class 0L - Invalid Grantor

        public const string InvalidGrantor = "0L000";
        public const string InvalidGrantOperation = "0LP01";

        #endregion Class 0L - Invalid Grantor

        #region Class 0P - Invalid Role Specification

        public const string InvalidRoleSpecification = "0P000";

        #endregion Class 0P - Invalid Role Specification

        #region Class 0Z - Diagnostics Exception

        public const string DiagnosticsException = "0Z000";
        public const string StackedDiagnosticsAccessedWithoutActiveHandler = "0Z002";

        #endregion Class 0Z - Diagnostics Exception

        #region Class 20 - Case Not Found

        public const string CaseNotFound = "20000";

        #endregion Class 20 - Case Not Found

        #region Class 21 - CardinalityViolation

        public const string CardinalityViolation = "21000";

        #endregion Class 21 - CardinalityViolation

        #region Class 22 - Data Exception

        public const string DataException = "22000";
        public const string ArraySubscriptError = "2202E";
        public const string CharacterNotInRepertoire = "22021";
        public const string DatetimeFieldOverflow = "22008";
        public const string DivisionByZero = "22012";
        public const string ErrorInAssignment = "22005";
        public const string EscapeCharacterConflict = "2200B";
        public const string IndicatorOverflow = "22022";
        public const string IntervalFieldOverflow = "22015";
        public const string InvalidArgumentForLogarithm = "2201E";
        public const string InvalidArgumentForNtileFunction = "22014";
        public const string InvalidArgumentForNthValueFunction = "22016";
        public const string InvalidArgumentForPowerFunction = "2201F";
        public const string InvalidArgumentForWidthBucketFunction = "2201G";
        public const string InvalidCharacterValueForCast = "22018";
        public const string InvalidDatetimeFormat = "22007";
        public const string InvalidEscapeCharacter = "22019";
        public const string InvalidEscapeOctet = "2200D";
        public const string InvalidEscapeSequence = "22025";
        public const string NonstandardUseOfEscapeCharacter = "22P06";
        public const string InvalidIndicatorParameterValue = "22010";
        public const string InvalidParameterValue = "22023";
        public const string InvalidRegularExpression = "2201B";
        public const string InvalidRowCountInLimitClause = "2201W";
        public const string InvalidRowCountInResultOffsetClause = "2201X";
        public const string InvalidTablesampleArgument = "2202H";
        public const string InvalidTablesampleRepeat = "2202G";
        public const string InvalidTimeZoneDisplacementValue = "22009";
        public const string InvalidUseOfEscapeCharacter = "2200C";
        public const string MostSpecificTypeMismatch = "2200G";
        public const string NullValueNotAllowed = "22004";
        public const string NullValueNoIndicatorParameter = "22002";
        public const string NumericValueOutOfRange = "22003";
        public const string StringDataLengthMismatch = "22026";
        public const string StringDataRightTruncation = "22001";
        public const string SubstringError = "22011";
        public const string TrimError = "22027";
        public const string UnterminatedCString = "22024";
        public const string ZeroLengthCharacterString = "2200F";
        public const string FloatingPointException = "22P01";
        public const string InvalidTextRepresentation = "22P02";
        public const string InvalidBinaryRepresentation = "22P03";
        public const string BadCopyFileFormat = "22P04";
        public const string UntranslatableCharacter = "22P05";
        public const string NotAnXmlDocument = "2200L";
        public const string InvalidXmlDocument = "2200M";
        public const string InvalidXmlContent = "2200N";
        public const string InvalidXmlComment = "2200S";
        public const string InvalidXmlProcessingInstruction = "2200T";

        #endregion Class 22 - Data Exception

        #region Class 23 - Integrity Constraint Violation

        public const string IntegrityConstraintViolation = "23000";
        public const string RestrictViolation = "23001";
        public const string NotNullViolation = "23502";
        public const string ForeignKeyViolation = "23503";
        public const string UniqueViolation = "23505";
        public const string CheckViolation = "23514";
        public const string ExclusionViolation = "23P01";

        #endregion Class 23 - Integrity Constraint Violation

        #region Class 24 - Invalid Cursor State

        public const string InvalidCursorState = "24000";

        #endregion Class 24 - Invalid Cursor State

        #region Class 25 - Invalid Transaction State

        public const string InvalidTransactionState = "25000";
        public const string ActiveSqlTransaction = "25001";
        public const string BranchTransactionAlreadyActive = "25002";
        public const string HeldCursorRequiresSameIsolationLevel = "25008";
        public const string InappropriateAccessModeForBranchTransaction = "25003";
        public const string InappropriateIsolationLevelForBranchTransaction = "25004";
        public const string NoActiveSqlTransactionForBranchTransaction = "25005";
        public const string ReadOnlySqlTransaction = "25006";
        public const string SchemaAndDataStatementMixingNotSupported = "25007";
        public const string NoActiveSqlTransaction = "25P01";
        public const string InFailedSqlTransaction = "25P02";

        #endregion Class 25 - Invalid Transaction State

        #region Class 26 - Invalid SQL Statement Name

        public const string InvalidSqlStatementName = "26000";

        #endregion Class 26 - Invalid SQL Statement Name

        #region Class 27 - Triggered Data Change Violation

        public const string TriggeredDataChangeViolation = "27000";

        #endregion Class 27 - Triggered Data Change Violation

        #region Class 28 - Invalid Authorization Scheme

        public const string InvalidAuthorizationSpecification = "28000";
        public const string InvalidPassword = "28P01";

        #endregion Class 28 - Invalid Authorization Scheme

        #region Class 2B - Dependent Privilege Descriptors Still Exist

        public const string DependentPrivilegeDescriptorsStillExist = "2B000";
        public const string DependentObjectsStillExist = "2BP01";

        #endregion Class 2B - Dependent Privilege Descriptors Still Exist

        #region Class 2D - Invalid Transaction Termination

        public const string InvalidTransactionTermination = "2D000";

        #endregion Class 2D - Invalid Transaction Termination

        #region Class 2F - SQL Routine Exception

        public const string SqlRoutineException = "2F000";
        public const string FunctionExecutedNoReturnStatementSqlRoutineException = "2F005";
        public const string ModifyingSqlDataNotPermittedSqlRoutineException = "2F002";
        public const string ProhibitedSqlStatementAttemptedSqlRoutineException = "2F003";
        public const string ReadingSqlDataNotPermittedSqlRoutineException = "2F004";

        #endregion Class 2F - SQL Routine Exception

        #region Class 34 - Invalid Cursor Name

        public const string InvalidCursorName = "34000";

        #endregion Class 34 - Invalid Cursor Name

        #region Class 38 - External Routine Exception

        public const string ExternalRoutineException = "38000";
        public const string ContainingSqlNotPermittedExternalRoutineException = "38001";
        public const string ModifyingSqlDataNotPermittedExternalRoutineException = "38002";
        public const string ProhibitedSqlStatementAttemptedExternalRoutineException = "38003";
        public const string ReadingSqlDataNotPermittedExternalRoutineException = "38004";

        #endregion Class 38 - External Routine Exception

        #region Class 39 - External Routine Invocation Exception

        public const string ExternalRoutineInvocationException = "39000";
        public const string InvalidSqlstateReturnedExternalRoutineInvocationException = "39001";
        public const string NullValueNotAllowedExternalRoutineInvocationException = "39004";
        public const string TriggerProtocolViolatedExternalRoutineInvocationException = "39P01";
        public const string SrfProtocolViolatedExternalRoutineInvocationException = "39P02";
        public const string EventTriggerProtocolViolatedExternalRoutineInvocationException = "39P03";

        #endregion Class 39 - External Routine Invocation Exception

        #region Class 3B - Savepoint Exception

        public const string SavepointException = "3B000";
        public const string InvalidSavepointSpecification = "3B001";

        #endregion Class 3B - Savepoint Exception

        #region Class 3D - Invalid Catalog Name

        public const string InvalidCatalogName = "3D000";

        #endregion Class 3D - Invalid Catalog Name

        #region Class 3F - Invalid Schema Name

        public const string InvalidSchemaName = "3F000";

        #endregion Class 3F - Invalid Schema Name

        #region Class 40 - Transaction Rollback

        public const string TransactionRollback = "40000";
        public const string TransactionIntegrityConstraintViolation = "40002";
        public const string SerializationFailure = "40001";
        public const string StatementCompletionUnknown = "40003";
        public const string DeadlockDetected = "40P01";

        #endregion Class 40 - Transaction Rollback

        #region Class 42 - Syntax Error or Access Rule Violation

        public const string SyntaxErrorOrAccessRuleViolation = "42000";
        public const string SyntaxError = "42601";
        public const string InsufficientPrivilege = "42501";
        public const string CannotCoerce = "42846";
        public const string GroupingError = "42803";
        public const string WindowingError = "42P20";
        public const string InvalidRecursion = "42P19";
        public const string InvalidForeignKey = "42830";
        public const string InvalidName = "42602";
        public const string NameTooLong = "42622";
        public const string ReservedName = "42939";
        public const string DatatypeMismatch = "42804";
        public const string IndeterminateDatatype = "42P18";
        public const string CollationMismatch = "42P21";
        public const string IndeterminateCollation = "42P22";
        public const string WrongObjectType = "42809";
        public const string UndefinedColumn = "42703";
        public const string UndefinedFunction = "42883";
        public const string UndefinedTable = "42P01";
        public const string UndefinedParameter = "42P02";
        public const string UndefinedObject = "42704";
        public const string DuplicateColumn = "42701";
        public const string DuplicateCursor = "42P03";
        public const string DuplicateDatabase = "42P04";
        public const string DuplicateFunction = "42723";
        public const string DuplicatePreparedStatement = "42P05";
        public const string DuplicateSchema = "42P06";
        public const string DuplicateTable = "42P07";
        public const string DuplicateAlias = "42712";
        public const string DuplicateObject = "42710";
        public const string AmbiguousColumn = "42702";
        public const string AmbiguousFunction = "42725";
        public const string AmbiguousParameter = "42P08";
        public const string AmbiguousAlias = "42P09";
        public const string InvalidColumnReference = "42P10";
        public const string InvalidColumnDefinition = "42611";
        public const string InvalidCursorDefinition = "42P11";
        public const string InvalidDatabaseDefinition = "42P12";
        public const string InvalidFunctionDefinition = "42P13";
        public const string InvalidPreparedStatementDefinition = "42P14";
        public const string InvalidSchemaDefinition = "42P15";
        public const string InvalidTableDefinition = "42P16";
        public const string InvalidObjectDefinition = "42P17";

        #endregion Class 42 - Syntax Error or Access Rule Violation

        #region Class 44 - WITH CHECK OPTION Violation

        public const string WithCheckOptionViolation = "44000";

        #endregion Class 44 - WITH CHECK OPTION Violation

        #region Class 53 - Insufficient Resources

        public const string InsufficientResources = "53000";
        public const string DiskFull = "53100";
        public const string OutOfMemory = "53200";
        public const string TooManyConnections = "53300";
        public const string ConfigurationLimitExceeded = "53400";

        #endregion Class 53 - Insufficient Resources

        #region Class 54 - Program Limit Exceeded

        public const string ProgramLimitExceeded = "54000";
        public const string StatementTooComplex = "54001";
        public const string TooManyColumns = "54011";
        public const string TooManyArguments = "54023";

        #endregion Class 54 - Program Limit Exceeded

        #region Class 55 - Object Not In Prerequisite State

        public const string ObjectNotInPrerequisiteState = "55000";
        public const string ObjectInUse = "55006";
        public const string CantChangeRuntimeParam = "55P02";
        public const string LockNotAvailable = "55P03";

        #endregion Class 55 - Object Not In Prerequisite State

        #region Class 57 - Operator Intervention

        public const string OperatorIntervention = "57000";
        public const string QueryCanceled = "57014";
        public const string AdminShutdown = "57P01";
        public const string CrashShutdown = "57P02";
        public const string CannotConnectNow = "57P03";
        public const string DatabaseDropped = "57P04";

        #endregion Class 57 - Operator Intervention

        #region Class 58 - System Error (errors external to PostgreSQL itself)

        public const string SystemError = "58000";
        public const string IoError = "58030";
        public const string UndefinedFile = "58P01";
        public const string DuplicateFile = "58P02";

        #endregion Class 58 - System Error (errors external to PostgreSQL itself)

        #region Class 72 - Snapshot Failure

        public const string SnapshotFailure = "72000";

        #endregion Class 72 - Snapshot Failure

        #region Class F0 - Configuration File Error

        public const string ConfigFileError = "F0000";
        public const string LockFileExists = "F0001";

        #endregion Class F0 - Configuration File Error

        #region Class HV - Foreign Data Wrapper Error (SQL/MED)

        public const string FdwError = "HV000";
        public const string FdwColumnNameNotFound = "HV005";
        public const string FdwDynamicParameterValueNeeded = "HV002";
        public const string FdwFunctionSequenceError = "HV010";
        public const string FdwInconsistentDescriptorInformation = "HV021";
        public const string FdwInvalidAttributeValue = "HV024";
        public const string FdwInvalidColumnName = "HV007";
        public const string FdwInvalidColumnNumber = "HV008";
        public const string FdwInvalidDataType = "HV004";
        public const string FdwInvalidDataTypeDescriptors = "HV006";
        public const string FdwInvalidDescriptorFieldIdentifier = "HV091";
        public const string FdwInvalidHandle = "HV00B";
        public const string FdwInvalidOptionIndex = "HV00C";
        public const string FdwInvalidOptionName = "HV00D";
        public const string FdwInvalidStringLengthOrBufferLength = "HV090";
        public const string FdwInvalidStringFormat = "HV00A";
        public const string FdwInvalidUseOfNullPointer = "HV009";
        public const string FdwTooManyHandles = "HV014";
        public const string FdwOutOfMemory = "HV001";
        public const string FdwNoSchemas = "HV00P";
        public const string FdwOptionNameNotFound = "HV00J";
        public const string FdwReplyHandle = "HV00K";
        public const string FdwSchemaNotFound = "HV00Q";
        public const string FdwTableNotFound = "HV00R";
        public const string FdwUnableToCreateExecution = "HV00L";
        public const string FdwUnableToCreateReply = "HV00M";
        public const string FdwUnableToEstablishConnection = "HV00N";

        #endregion Class HV - Foreign Data Wrapper Error (SQL/MED)

        #region Class P0 - PL/pgSQL Error

        public const string PlpgsqlError = "P0000";
        public const string RaiseException = "P0001";
        public const string NoDataFound = "P0002";
        public const string TooManyRows = "P0003";
        public const string AssertFailure = "P0004";

        #endregion Class P0 - PL/pgSQL Error

        #region Class XX - Internal Error

        public const string InternalError = "XX000";
        public const string DataCorrupted = "XX001";
        public const string IndexCorrupted = "XX002";

        #endregion Class XX - Internal Error
    }
}
