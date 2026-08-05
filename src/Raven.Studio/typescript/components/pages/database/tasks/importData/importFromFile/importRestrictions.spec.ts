import { connectionStringRules, databaseSettingRules, ongoingTaskRules, resolveRestriction } from "./importRestrictions";

const fullLicense = {
    HasQueueEtl: true,
    HasQueueSink: true,
    HasGenAi: true,
    HasAiAgent: true,
    HasRavenEtl: true,
    HasPullReplicationAsHub: true,
} as LicenseStatus;

const context = (overrides?: {
    licenseStatus?: Partial<LicenseStatus>;
    isSharded?: boolean;
    deniedAccess?: boolean;
    isShardingChecked?: boolean;
}) => ({
    licenseStatus: { ...fullLicense, ...overrides?.licenseStatus } as LicenseStatus,
    isSharded: overrides?.isSharded ?? false,
    canHandleOperation: () => !overrides?.deniedAccess,
    isShardingChecked: overrides?.isShardingChecked,
});

describe("resolveRestriction", () => {
    it("returns null for an ungated entry", () => {
        expect(resolveRestriction(ongoingTaskRules.queueEtls, context())).toBeNull();
    });

    it("returns null when no rule exists", () => {
        expect(resolveRestriction(undefined, context())).toBeNull();
    });

    it("reports a license gap with its badge", () => {
        const result = resolveRestriction(
            ongoingTaskRules.queueEtls,
            context({ licenseStatus: { HasQueueEtl: false } })
        );
        expect(result?.reason).toBe("license");
        expect(result?.licenseRequired).toBe("Enterprise");
    });

    it("reports sharding for a task that does not support it", () => {
        // hubReplications has no isShardingSupported flag
        const result = resolveRestriction(ongoingTaskRules.hubReplications, context({ isSharded: true }));
        expect(result?.reason).toBe("sharding");
        expect(result?.licenseRequired).toBeUndefined();
    });

    it("allows a sharding-supported task on a sharded database", () => {
        expect(resolveRestriction(ongoingTaskRules.ravenEtls, context({ isSharded: true }))).toBeNull();
    });

    it("reports sharding for PostgreSQL integration when resolved with the sharding-checked context", () => {
        // the server strips PostgreSQLIntegration from sharded imports, so useImportRestrictions
        // resolves this one settings entry with the sharding-checked (task) context
        const result = resolveRestriction(
            databaseSettingRules.postgreSqlIntegration,
            context({ licenseStatus: { HasPostgreSqlIntegration: true }, isSharded: true })
        );
        expect(result?.reason).toBe("sharding");
    });

    it("skips the sharding check when the group opts out", () => {
        expect(
            resolveRestriction(
                connectionStringRules.ravenConnectionStrings,
                context({ isSharded: true, isShardingChecked: false })
            )
        ).toBeNull();
    });

    it("reports insufficient access", () => {
        const result = resolveRestriction(ongoingTaskRules.queueEtls, context({ deniedAccess: true }));
        expect(result?.reason).toBe("access");
    });

    it("prefers the license reason - it is the only one the user can act on", () => {
        const result = resolveRestriction(
            ongoingTaskRules.hubReplications,
            context({ licenseStatus: { HasPullReplicationAsHub: false }, isSharded: true, deniedAccess: true })
        );
        expect(result?.reason).toBe("license");
    });

    describe("multi-flag entries", () => {
        it("keeps queue connection strings available with only the Sink flag", () => {
            expect(
                resolveRestriction(
                    connectionStringRules.queueConnectionStrings,
                    context({ licenseStatus: { HasQueueEtl: false }, isShardingChecked: false })
                )
            ).toBeNull();
        });

        it("restricts queue connection strings when both queue flags are missing", () => {
            const result = resolveRestriction(
                connectionStringRules.queueConnectionStrings,
                context({ licenseStatus: { HasQueueEtl: false, HasQueueSink: false }, isShardingChecked: false })
            );
            expect(result?.reason).toBe("license");
        });

        it("keeps AI connection strings available with only one AI feature", () => {
            expect(
                resolveRestriction(
                    connectionStringRules.aiConnectionStrings,
                    context({
                        licenseStatus: { HasGenAi: false, HasAiAgent: false, HasEmbeddingsGeneration: true },
                        isShardingChecked: false,
                    })
                )
            ).toBeNull();
        });

        it("restricts AI connection strings when every AI feature is missing", () => {
            const result = resolveRestriction(
                connectionStringRules.aiConnectionStrings,
                context({
                    licenseStatus: { HasGenAi: false, HasAiAgent: false, HasEmbeddingsGeneration: false },
                    isShardingChecked: false,
                })
            );
            expect(result?.reason).toBe("license");
        });
    });
});
