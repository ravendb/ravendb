import recentError from "common/notifications/models/recentError";
import virtualBulkInsert from "common/notifications/models/virtualBulkInsert";
import virtualBulkInsertFailures from "common/notifications/models/virtualBulkInsertFailures";
import virtualDeleteByQuery from "common/notifications/models/virtualDeleteByQuery";
import virtualDeleteByQueryFailures from "common/notifications/models/virtualDeleteByQueryFailures";
import virtualUpdateByQuery from "common/notifications/models/virtualUpdateByQuery";
import virtualUpdateByQueryFailures from "common/notifications/models/virtualUpdateByQueryFailures";
import {
    createDetailsProvider,
    isAlert,
    isInstanceOf,
    isOperation,
    isPerformanceHint,
} from "./notificationDetailsProvider";
import RecentErrorDetails from "./RecentErrorDetails";
import DumpRawIndexDataDetails from "./operations/DumpRawIndexDataDetails";
import {
    VirtualBulkInsertDetails,
    VirtualBulkInsertFailuresDetails,
    VirtualDeleteByQueryDetails,
    VirtualDeleteByQueryFailuresDetails,
    VirtualUpdateByQueryDetails,
    VirtualUpdateByQueryFailuresDetails,
} from "./virtualOperations/VirtualOperationDetails";
import HugeDocumentsDetails from "./performanceHints/HugeDocumentsDetails";
import IndexingDetails from "./performanceHints/IndexingDetails";
import IndexingReferencesDetails from "./performanceHints/IndexingReferencesDetails";
import PagingDetails from "./performanceHints/PagingDetails";
import RequestLatencyDetails from "./performanceHints/RequestLatencyDetails";
import SlowIoDetails from "./performanceHints/SlowIoDetails";
import SlowSqlDetails from "./performanceHints/SlowSqlDetails";
import AiAgentExceededTokenThresholdDetails from "./alerts/AiAgentExceededTokenThresholdDetails";
import BlockingTombstonesDetails from "./alerts/BlockingTombstonesDetails";
import ComplexFieldsAlertDetails from "./alerts/ComplexFieldsAlertDetails";
import ConflictExceededDetails from "./alerts/ConflictExceededDetails";
import CpuCreditsBalanceDetails from "./alerts/CpuCreditsBalanceDetails";
import EtlTaskHealthChangeDetails from "./alerts/EtlTaskHealthChangeDetails";
import GenericAlertDetails from "./alerts/GenericAlertDetails";
import MismatchedReferenceLoadDetails from "./alerts/MismatchedReferenceLoadDetails";
import NewVersionAvailableDetails from "./alerts/NewVersionAvailableDetails";
import QueueSinkErrorDetails from "./alerts/QueueSinkErrorDetails";
import RemoteAttachmentErrorDetails from "./alerts/RemoteAttachmentErrorDetails";
import ServerLimitsDetails from "./alerts/ServerLimitsDetails";

const notificationDetailsProviders = [
    createDetailsProvider(isInstanceOf(recentError), RecentErrorDetails),

    createDetailsProvider(isOperation("DumpRawIndexData"), DumpRawIndexDataDetails),

    createDetailsProvider(isInstanceOf(virtualBulkInsert), VirtualBulkInsertDetails),
    createDetailsProvider(isInstanceOf(virtualBulkInsertFailures), VirtualBulkInsertFailuresDetails),
    createDetailsProvider(isInstanceOf(virtualUpdateByQuery), VirtualUpdateByQueryDetails),
    createDetailsProvider(isInstanceOf(virtualUpdateByQueryFailures), VirtualUpdateByQueryFailuresDetails),
    createDetailsProvider(isInstanceOf(virtualDeleteByQuery), VirtualDeleteByQueryDetails),
    createDetailsProvider(isInstanceOf(virtualDeleteByQueryFailures), VirtualDeleteByQueryFailuresDetails),

    createDetailsProvider(isPerformanceHint("Indexing"), IndexingDetails),
    createDetailsProvider(isPerformanceHint("SqlEtl_SlowSql"), SlowSqlDetails),
    createDetailsProvider(isPerformanceHint("Indexing_References"), IndexingReferencesDetails),
    createDetailsProvider(isPerformanceHint("SlowIO"), SlowIoDetails),
    createDetailsProvider(isPerformanceHint("Paging"), PagingDetails),
    createDetailsProvider(isPerformanceHint("RequestLatency"), RequestLatencyDetails),
    createDetailsProvider(isPerformanceHint("HugeDocuments"), HugeDocumentsDetails),

    createDetailsProvider(isAlert("Server_NewVersionAvailable"), NewVersionAvailableDetails),
    createDetailsProvider(isAlert("MismatchedReferenceLoad"), MismatchedReferenceLoadDetails),
    createDetailsProvider(isAlert("BlockingTombstones"), BlockingTombstonesDetails),
    createDetailsProvider(isAlert("QueueSink_ConsumeError", "QueueSink_ScriptError"), QueueSinkErrorDetails),
    createDetailsProvider(isAlert("Throttling_CpuCreditsBalance"), CpuCreditsBalanceDetails),
    createDetailsProvider(isAlert("ServerLimits"), ServerLimitsDetails),
    createDetailsProvider(isAlert("ConflictRevisionsExceeded"), ConflictExceededDetails),
    createDetailsProvider(isAlert("Indexing_CoraxComplexItem"), ComplexFieldsAlertDetails),
    createDetailsProvider(isAlert("AiAgent_ExceededTokenThreshold"), AiAgentExceededTokenThresholdDetails),
    createDetailsProvider(isAlert("Attachments_RemoteAttachmentErroredIdentifier"), RemoteAttachmentErrorDetails),
    createDetailsProvider(isAlert("Etl_HealthStatusChange"), EtlTaskHealthChangeDetails),
];

export const genericAlertDetailsProvider = createDetailsProvider(isAlert(), GenericAlertDetails);

export default notificationDetailsProviders;
