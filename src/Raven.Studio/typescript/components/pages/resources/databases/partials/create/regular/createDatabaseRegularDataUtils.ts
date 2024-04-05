import { CreateDatabaseRegularFormData as FormData } from "./createDatabaseRegularValidation";
import { CreateDatabaseDto } from "commands/resources/createDatabaseCommand";

const getDefaultValues = (replicationFactor: number, allNodeTags: string[]): FormData => {
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
            replicationFactor,
            isSharded: false,
            shardsCount: 1,
            isDynamicDistribution: false,
            isManualReplication: false,
        },
        manualNodeSelectionStep: {
            nodes: allNodeTags,
            shards: [],
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

export const createDatabaseRegularDataUtils = {
    getDefaultValues,
    mapToDto,
};
