import { CreateDatabaseRegularFormData as FormData } from "./createDatabaseRegularValidation";
import { CreateDatabaseDto } from "commands/resources/createDatabaseCommand";

const getDefaultValues = (allNodeTags: string[]): FormData => {
    return {
        basicInfoStep: {
            databaseName: "",
            isEncrypted: false,
        },
        encryptionStep: {
            key: "",
            isKeySaved: false,
        },
        replicationAndShardingStep: {
            replicationFactor: allNodeTags.length || 1,
            isSharded: false,
            shardsCount: 1,
            isDynamicDistribution: false,
            isManualReplication: false,
            isPrefixesForShards: false,
        },
        manualNodeSelectionStep: {
            // if there is only one node, it should be selected by default
            nodes: allNodeTags.length === 1 ? allNodeTags : [],
            shards: [],
        },
        shardingPrefixesStep: {
            prefixesForShards: [
                {
                    prefix: "",
                    shardNumbers: [],
                },
            ],
        },
        dataDirectoryStep: {
            isDefault: true,
            directory: "",
        },
    };
};

function mapToDto(formValues: FormData, allNodeTags: string[]): CreateDatabaseDto {
    const { basicInfoStep, replicationAndShardingStep, manualNodeSelectionStep, dataDirectoryStep } = formValues;

    const Settings: CreateDatabaseDto["Settings"] = dataDirectoryStep.isDefault
        ? {}
        : {
              DataDir: dataDirectoryStep.directory,
          };

    const Topology: CreateDatabaseDto["Topology"] = replicationAndShardingStep.isSharded
        ? null
        : {
              Members: replicationAndShardingStep.isManualReplication ? manualNodeSelectionStep.nodes : null,
              DynamicNodesDistribution: replicationAndShardingStep.isDynamicDistribution,
          };

    const Shards: CreateDatabaseDto["Sharding"]["Shards"] = {};

    if (replicationAndShardingStep.isSharded) {
        for (let i = 0; i < replicationAndShardingStep.shardsCount; i++) {
            Shards[i] = replicationAndShardingStep.isManualReplication
                ? {
                      Members: manualNodeSelectionStep.shards[i].filter((x) => x),
                  }
                : {};
        }
    }

    const selectedOrchestrators =
        formValues.replicationAndShardingStep.isSharded && formValues.replicationAndShardingStep.isManualReplication
            ? formValues.manualNodeSelectionStep.nodes
            : allNodeTags;

    const Sharding: CreateDatabaseDto["Sharding"] = replicationAndShardingStep.isSharded
        ? {
              Shards,
              Orchestrator: {
                  Topology: {
                      Members: selectedOrchestrators,
                  },
              },
              Prefixed: formValues.replicationAndShardingStep.isPrefixesForShards
                  ? formValues.shardingPrefixesStep.prefixesForShards.map((x) => ({
                        Prefix: x.prefix,
                        Shards: x.shardNumbers,
                    }))
                  : null,
          }
        : null;

    return {
        DatabaseName: basicInfoStep.databaseName,
        Settings,
        Disabled: false,
        Encrypted: basicInfoStep.isEncrypted,
        Topology,
        Sharding,
    };
}

function getIsManualReplicationRequiredForEncryption(nodeTagsCount: number, isEncrypted: boolean): boolean {
    return nodeTagsCount > 1 && isEncrypted;
}

export const createDatabaseRegularDataUtils = {
    getDefaultValues,
    mapToDto,
    getIsManualReplicationRequiredForEncryption,
};
