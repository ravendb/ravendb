import {
    databaseAccessArgType,
    licenseArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import ImportDatabaseFromFile from "../ImportDatabaseFromFile";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Tasks/Import Data/Import From File",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        databaseAccess: databaseAccessArgType,
        licenseType: licenseArgType,
    },
} satisfies Meta;

interface ImportFromFileStoryArgs {
    databaseAccess: databaseAccessLevel;
    licenseType: Raven.Server.Commercial.LicenseType;
}

type LicenseStatus = Raven.Server.Commercial.LicenseStatus;

interface StoryOptions {
    databaseAccess?: databaseAccessLevel;
    licenseType?: Raven.Server.Commercial.LicenseType;
    license?: Partial<LicenseStatus>;
    isSharded?: boolean;
    /** 0 documents + 0 indexes = empty database */
    isEmptyDatabase?: boolean;
    /** omits the essential-stats response entirely (empty body / 204) */
    isStatsUnavailable?: boolean;
    upload?: { uploadDurationMs?: number; failUpload?: boolean; validationError?: string };
}

function init({
    databaseAccess = "DatabaseAdmin",
    licenseType = "Enterprise",
    license: licenseOverrides,
    isSharded = false,
    isEmptyDatabase = false,
    isStatsUnavailable = false,
    upload,
}: StoryOptions = {}) {
    const { accessManager, databases, license, collectionsTracker } = mockStore;
    const { tasksService, databasesService } = mockServices;

    const db = isSharded
        ? databases.withActiveDatabase_Sharded()
        : databases.withActiveDatabase_NonSharded_SingleNode();

    accessManager.with_databaseAccess({ [db.name]: databaseAccess });
    accessManager.with_securityClearance("ValidUser");
    license.with_LicenseLimited({ Type: licenseType, ...licenseOverrides });
    collectionsTracker.with_Collections();

    if (isStatsUnavailable) {
        // withEssentialStats falls back to the stub on a nullish value (createValue does
        // `value ?? defaultValue`), so an empty body has to be mocked explicitly
        databasesService.withEmptyEssentialStats();
    } else {
        databasesService.withEssentialStats((dto) => {
            if (isEmptyDatabase) {
                dto.CountOfDocuments = 0;
                dto.CountOfIndexes = 0;
                dto.Indexes = [];
            }
        });
    }

    tasksService.withImportDatabaseFromFile(upload);
}

const story = (name: string, description: string, options: StoryOptions = {}): StoryObj<ImportFromFileStoryArgs> => ({
    name,
    render: (props) => {
        init({ ...options, databaseAccess: props.databaseAccess, licenseType: props.licenseType });
        return <ImportDatabaseFromFile />;
    },
    args: {
        databaseAccess: options.databaseAccess ?? "DatabaseAdmin",
        licenseType: options.licenseType ?? "Enterprise",
    },
    parameters: { docs: { description: { story: description } } },
});

// ---------------------------------------------------------------------------
// Baseline
// ---------------------------------------------------------------------------

export const Story00_Default = story(
    "00 - Default (everything available)",
    "Enterprise + DatabaseAdmin + non-sharded, database has data. EXPECT: no license/sharding/access " +
        "chips under the file input; every toggle enabled; the orange overwrite warning IS shown; " +
        '"Customize" panels start collapsed (nothing is restricted).'
);

export const Story01a_EmptyDatabase = story(
    "01a - Empty database (no overwrite warning)",
    "CountOfDocuments = 0 and CountOfIndexes = 0. EXPECT: the " +
        '"Importing will overwrite any existing documents and indexes" alert is ABSENT - nothing can be overwritten.',
    { isEmptyDatabase: true }
);

export const Story01b_NonEmptyDatabase = story(
    "01b - Non-empty database (warning is a warning)",
    "Database has 1.2M documents and 17 indexes. EXPECT: the overwrite alert IS shown and is styled as " +
        "a WARNING (orange, warning triangle) rather than informational blue."
);

export const Story01c_StatsUnavailable = story(
    "01c - Stats response missing (must not crash)",
    "getEssentialStats resolves with no body, the way a 204 would. EXPECT: the page RENDERS and the " +
        "overwrite warning is shown (safe default). This is the regression that threw " +
        '"Cannot read properties of undefined (reading \'CountOfDocuments\')".',
    { isStatsUnavailable: true }
);

export const Story02_NoBlanketLicenseAlert = story(
    "02 - Full license (no speculative alert)",
    'Full Enterprise license. EXPECT: the old always-on blue alert ("Your import might contain settings ' +
        'that aren\'t available on your current license") is GONE. Compare with 03a, where a real chip list appears instead.'
);

export const Story03a_CommunityLicense = story(
    "03a - Community license (many features missing)",
    'EXPECT: yellow "Some data may not be imported" alert listing every gated feature as a chip; ' +
        "the gated rows inside Customize are disabled with a license badge and a tooltip; " +
        'BOTH "Customize" panels are auto-expanded (see 08); a "See license comparison" link is present.',
    {
        licenseType: "Community",
        license: {
            HasDocumentsCompression: false,
            HasDataArchival: false,
            HasTimeSeriesRollupsAndRetention: false,
            HasPostgreSqlIntegration: false,
            HasClientConfiguration: false,
            HasSchemaValidation: false,
            HasPeriodicBackup: false,
            HasExternalReplication: false,
            HasRavenEtl: false,
            HasSqlEtl: false,
            HasSnowflakeEtl: false,
            HasOlapEtl: false,
            HasElasticSearchEtl: false,
            HasQueueEtl: false,
            HasQueueSink: false,
            HasPullReplicationAsHub: false,
            HasPullReplicationAsSink: false,
            HasEmbeddingsGeneration: false,
            HasGenAi: false,
            HasAiAgent: false,
            HasCdcSink: false,
            HasRemoteAttachments: false,
        },
    }
);

export const Story03b_ArchivedDocumentsGated = story(
    "03b - No Data Archival (archived documents toggle)",
    'HasDataArchival = false. EXPECT: in "Data to import", the "Include Archived Documents" toggle is ' +
        'disabled with a license badge, and "Select all" SKIPS it (clicking Select all must not check it). ' +
        '"Data Archival" also appears as a chip and as a disabled database setting.',
    { license: { HasDataArchival: false } }
);

export const Story03c_CollectionsPicker = story(
    "03c - Collections picker (typed in by hand)",
    'MANUAL STEPS: click "Customize imported collections". EXPECT: a text input + Add button (NOT a select - ' +
        "the file's collections are unknown before upload). Type a name and press ENTER - the row is added and " +
        "the input clears. Click Add with the field EMPTY - expect the error \"Enter a collection name\". Add the " +
        'same name twice - expect "This collection is already on the list". Once rows exist, a separate ' +
        '"Filter added collections" input appears; filtering to nothing shows "No added collection matches the ' +
        'filter". The trash icon removes a row; the toggle only deselects it.'
);

export const Story04_MissingConnectionString = story(
    "04 - Task without its connection string",
    'MANUAL STEPS: click "Customize" next to "Include Connection Strings & Ongoing Tasks", then uncheck ' +
        '"SQL Connection Strings" while leaving "SQL ETLs" checked. EXPECT: a warning appears naming SQL ETLs ' +
        '("...imported but won\'t run until a matching connection string exists"). Re-check it and the warning ' +
        "disappears. Also verify: it does NOT appear while Customize is off (everything is imported together)."
);

export const Story05_TransformScriptValidation = story(
    "05 - Transform script is validated locally",
    'MANUAL STEPS: go to "Import processing & security", enable "Use transform script", then break the script ' +
        '(e.g. "this.Freight = ;"). EXPECT: an "Invalid JavaScript: ..." error appears WITHOUT any request being ' +
        "sent. Valid function-body syntax (this / return / throw at top level) must be accepted - the default " +
        "script and \"if (this.Name === 'Bob') throw 'skip';\" are both valid."
);

export const Story06_InvalidInputJumpsToError = story(
    "06 - Invalid input: button enabled, jumps to the field",
    'MANUAL STEPS: pick a file, then enable "Use transform script" and clear the editor (or enable ' +
        '"Imported file is encrypted" and leave the key empty). Scroll back to the top and click ' +
        '"Import database". EXPECT: the button is ENABLED (not greyed out); clicking it does NOT start an ' +
        "import; the offending section is scrolled to and its collapsed panel is opened so the error message " +
        "is visible. Previously the button was disabled with the error hidden."
);

export const Story07a_ShardedDatabase = story(
    "07a - Sharded database (unsupported tasks gated)",
    "Sharded DB, full Enterprise license. EXPECT: tasks that do not support sharding (Replication Hub, " +
        "Replication Sink, Queue ETLs, GenAI, AI Agents, CDC Sinks, Remote Attachments) are DISABLED with a " +
        "SHARDING tooltip and NO license badge. Sharding-supported tasks (RavenDB ETL, SQL ETL, Periodic Backup, " +
        'External Replication, Embeddings Generation) stay ENABLED. A separate "Some data cannot be imported" ' +
        "alert lists them with a sharding icon. Database settings and connection strings must NOT be gated by sharding.",
    { isSharded: true }
);

export const Story07b_ReadWriteAccess = story(
    "07b - DatabaseReadWrite certificate (admin-only tasks gated)",
    "Non-admin certificate. EXPECT: every ongoing task and connection string requiring DatabaseAdmin is " +
        'DISABLED with a CERTIFICATE tooltip and no license badge, listed in the "Some data cannot be imported" ' +
        "alert. Previously access level only changed the default checkbox state - the rows stayed clickable.",
    { databaseAccess: "DatabaseReadWrite" }
);

export const Story07c_LicenseShardingAccess = story(
    "07c - License + sharding + access all at once (license wins)",
    "Community license, sharded DB, read-write certificate. EXPECT: for a row gated by more than one reason, " +
        "the LICENSE reason is reported (it is the only one the user can act on), so it shows a license badge. " +
        "Both alert boxes appear: the license chip list and the sharding/certificate chip list.",
    {
        isSharded: true,
        databaseAccess: "DatabaseReadWrite",
        licenseType: "Community",
        license: { HasPullReplicationAsHub: false, HasQueueEtl: false, HasQueueSink: false },
    }
);

export const Story08_AutoExpandOnRestriction = story(
    "08 - Customize auto-expands when something is gated",
    'Only HasOlapEtl = false. EXPECT: both "Customize" panels are ALREADY expanded on load, so the disabled ' +
        '"OLAP ETLs" row is visible without clicking. Compare with 00, where a full license leaves them collapsed.',
    { license: { HasOlapEtl: false } }
);

export const Story09_AiAgentFlag = story(
    "09 - HasAiAgent is now respected",
    'Only HasAiAgent = false. EXPECT: the "AI Agents" ongoing-task row is disabled with an "Enterprise AI" ' +
        "badge. Before the fix this flag was not checked at all and the row stayed enabled.",
    { license: { HasAiAgent: false } }
);

export const Story10a_ConnectionStringsGated = story(
    "10a - Connection strings follow license flags",
    "HasSqlEtl / HasOlapEtl / HasElasticSearchEtl = false. EXPECT: inside Customize, the matching " +
        "connection-string rows (SQL, OLAP, Elasticsearch) are DISABLED with badges - previously they were " +
        'always enabled. "Select all" over connection strings must skip them.',
    { license: { HasSqlEtl: false, HasOlapEtl: false, HasElasticSearchEtl: false } }
);

export const Story10b_QueueSinkOnly = story(
    "10b - Queue connection string: ETL off, Sink ON (must stay ENABLED)",
    "HasQueueEtl = false but HasQueueSink = true. One connection-string type serves both features, so " +
        'EXPECT: "Queue Connection Strings" stays ENABLED (the license can still use it for Sink), while the ' +
        '"Queue ETLs" TASK row is disabled. This is the multi-flag rule - the interesting case.',
    { license: { HasQueueEtl: false, HasQueueSink: true } }
);

export const Story10c_QueueBothOff = story(
    "10c - Queue connection string: ETL and Sink both off (must be DISABLED)",
    'Both queue flags false. EXPECT: "Queue Connection Strings" IS disabled with a badge. Contrast with 10b: ' +
        "only when every backing flag is missing does the connection string get gated.",
    { license: { HasQueueEtl: false, HasQueueSink: false } }
);

export const Story10d_AiPartial = story(
    "10d - AI connection string: only Embeddings left (must stay ENABLED)",
    "HasGenAi = false, HasAiAgent = false, HasEmbeddingsGeneration = TRUE. EXPECT: " +
        '"AI Connection Strings" stays ENABLED because Embeddings Generation can still use it, even though ' +
        "the GenAI and AI Agents task rows are disabled.",
    { license: { HasGenAi: false, HasAiAgent: false, HasEmbeddingsGeneration: true } }
);

export const Story10e_AiAllOff = story(
    "10e - AI connection string: no AI features at all (must be DISABLED)",
    'All three AI flags false. EXPECT: "AI Connection Strings" IS disabled with an "Enterprise AI" badge.',
    { license: { HasGenAi: false, HasAiAgent: false, HasEmbeddingsGeneration: false } }
);

export const Story11_UploadProgressRounded = story(
    "11 - Upload progress label is a whole number",
    'MANUAL STEPS: pick any .ravendbdump file and click "Import database". The upload is simulated over ~10s. ' +
        'EXPECT: the progress bar label reads a WHOLE number ("45%"), never "37.421052631578945%". The mock ' +
        "deliberately reports unrounded values, so this only passes because the view rounds them.",
    { upload: { uploadDurationMs: 10_000 } }
);

export const Story12_UploadBlocksNavigation = story(
    "12 - Navigation is blocked during upload",
    "MANUAL STEPS: start an import (simulated over ~20s), then try to navigate away in the app while the " +
        'progress bar is moving. EXPECT: an OK-only dialog "Upload is in progress / Please wait until uploading ' +
        'is complete." and you STAY on the page. It must not be a "discard changes?" prompt that lets you leave.',
    { upload: { uploadDurationMs: 20_000 } }
);

export const Story13_UploadFailure = story(
    "13 - Upload failure marks the operation faulted",
    "MANUAL STEPS: start an import. The simulated upload fails after ~3s. EXPECT: the result modal shows a " +
        "FAULTED state rather than spinning forever, and navigation is possible again afterwards.",
    { upload: { uploadDurationMs: 3_000, failUpload: true } }
);

export const Story14_ValidationRejected = story(
    "14 - Server rejects the smuggler options",
    'MANUAL STEPS: start an import. EXPECT: an "Invalid import options" error notification, NO operation is ' +
        "started, and no progress bar appears.",
    { upload: { validationError: "TransformScript is not a valid JavaScript function" } }
);
